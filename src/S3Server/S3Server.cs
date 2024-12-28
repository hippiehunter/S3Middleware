using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Threading.Tasks;
using System.Globalization;
using System.Text;
using AWSSignatureGenerator;
using Microsoft.AspNetCore.Http.Extensions;
using S3ServerLibrary;
using S3ServerLibrary.Callbacks;
using S3ServerLibrary.S3Objects;
using Microsoft.Extensions.DependencyInjection;

namespace S3ServerLibrary
{
    public class S3RequestHandler
    {
        private readonly RequestDelegate _next;
        private readonly ILogger<S3RequestHandler> _logger;
        private readonly S3ServerSettings _settings;
        private const string _header = "[S3Server] ";
        
        private readonly ServiceCallbacks _serviceCallbacks;
        private readonly BucketCallbacks _bucketCallbacks;
        private readonly ObjectCallbacks _objectCallbacks;

        public S3RequestHandler(
            RequestDelegate next,
            ILogger<S3RequestHandler> logger,
            S3ServerSettings settings,
            ServiceCallbacks serviceCallbacks,
            BucketCallbacks bucketCallbacks,
            ObjectCallbacks objectCallbacks)
        {
            _next = next ?? throw new ArgumentNullException(nameof(next));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _settings = settings ?? throw new ArgumentNullException(nameof(settings));
            _serviceCallbacks = serviceCallbacks ?? throw new ArgumentNullException(nameof(serviceCallbacks));
            _bucketCallbacks = bucketCallbacks ?? throw new ArgumentNullException(nameof(bucketCallbacks));
            _objectCallbacks = objectCallbacks ?? throw new ArgumentNullException(nameof(objectCallbacks));
        }

        public async Task InvokeAsync(HttpContext context)
        {
            S3Context s3ctx = null;

            try
            {
                s3ctx = new S3Context(
                    context,
                    _serviceCallbacks.FindMatchingBaseDomain,
                    null,
                    _settings.Logging.S3Requests ? (Action<string>)LogMessage : null);

                await ConfigureResponseHeaders(context, s3ctx);
                await LogRequestDetails(s3ctx);

                if (await HandlePreRequest(context, s3ctx))
                {
                    return;
                }

                if (_settings.EnableSignatures)
                {
                    await ValidateSignature(s3ctx);
                }

                await ProcessS3Request(context, s3ctx);
            }
            catch (S3Exception s3e)
            {
                await HandleS3Exception(context, s3ctx, s3e);
            }
            catch (Exception e)
            {
                await HandleGenericException(context, s3ctx, e);
            }
            finally
            {
                await HandleRequestCompletion(s3ctx);
            }
        }

        private async Task ConfigureResponseHeaders(HttpContext context, S3Context s3ctx)
        {
            context.Response.Headers.Append(Constants.HeaderRequestId, s3ctx.Request.RequestId);
            context.Response.Headers.Append(Constants.HeaderTraceId, s3ctx.Request.TraceId);
            context.Response.Headers.Append(Constants.HeaderConnection, "close");
        }

        private async Task LogRequestDetails(S3Context s3ctx)
        {
            if (_settings.Logging.HttpRequests)
            {
                LogMessage($"HTTP request: {Environment.NewLine}{SerializationHelper.SerializeJson(s3ctx.Http, true)}");
            }

            if (_settings.Logging.S3Requests)
            {
                LogMessage($"S3 request: {Environment.NewLine}{SerializationHelper.SerializeJson(s3ctx.Request, true)}");
            }
        }

        private async Task<bool> HandlePreRequest(HttpContext context, S3Context s3ctx)
        {
            if (_settings.PreRequestHandler != null)
            {
                var success = await _settings.PreRequestHandler(s3ctx);
                if (success)
                {
                    await SendResponse(context, s3ctx.Response);
                    return true;
                }
            }
            return false;
        }

        private async Task ProcessS3Request(HttpContext context, S3Context s3ctx)
        {
            switch (s3ctx.Request.RequestType)
            {
                case S3RequestType.ServiceExists:
                    await HandleServiceExists(context, s3ctx);
                    break;

                case S3RequestType.ListBuckets:
                    await HandleListBuckets(context, s3ctx);
                    break;

                // Add other case handlers here...
                
                default:
                    if (_settings.DefaultRequestHandler != null)
                    {
                        await _settings.DefaultRequestHandler(s3ctx);
                    }
                    else
                    {
                        await SendErrorResponse(context, ErrorCode.InvalidRequest);
                    }
                    break;
            }
        }

        private async Task HandleServiceExists(HttpContext context, S3Context s3ctx)
        {
            if (_serviceCallbacks.ServiceExists != null)
            {
                string region = await _serviceCallbacks.ServiceExists(s3ctx);
                if (!string.IsNullOrEmpty(region))
                {
                    context.Response.Headers.Append(Constants.HeaderBucketRegion, region);
                }

                context.Response.StatusCode = StatusCodes.Status200OK;
                await SendResponse(context, s3ctx.Response);
            }
            else
            {
                await SendErrorResponse(context, ErrorCode.InvalidRequest);
            }
        }

        private async Task HandleListBuckets(HttpContext context, S3Context s3ctx)
        {
            if (_serviceCallbacks.ListBuckets != null)
            {
                var buckets = await _serviceCallbacks.ListBuckets(s3ctx);
                context.Response.StatusCode = StatusCodes.Status200OK;
                context.Response.ContentType = Constants.ContentTypeXml;
                await SendXmlResponse(context, buckets);
            }
            else
            {
                await SendErrorResponse(context, ErrorCode.InvalidRequest);
            }
        }

        private async Task ValidateSignature(S3Context s3ctx)
        {
            if (_serviceCallbacks.GetSecretKey == null)
            {
                return;
            }

            string secretKey = _serviceCallbacks.GetSecretKey(s3ctx);
            if (string.IsNullOrEmpty(secretKey))
            {
                _logger.LogWarning("Unable to retrieve secret key for signature {Signature}", s3ctx.Request.Signature);
                throw new S3Exception(new Error(ErrorCode.AccessDenied));
            }

            switch (s3ctx.Request.SignatureVersion)
            {
                case S3SignatureVersion.Version2:
                    _logger.LogWarning("Invalid v2 signature '{Signature}'", s3ctx.Request.Signature);
                    throw new S3Exception(new Error(ErrorCode.SignatureDoesNotMatch));

                case S3SignatureVersion.Version4:
                    await ValidateV4Signature(s3ctx, secretKey);
                    break;

                default:
                    _logger.LogWarning("Unknown signature version");
                    throw new S3Exception(new Error(ErrorCode.AccessDenied));
            }
        }

        private async Task ValidateV4Signature(S3Context s3ctx, string secretKey)
        {
            var headers = new System.Collections.Specialized.NameValueCollection();
            foreach (var header in s3ctx.Http.Request.Headers)
            {
                headers.Add(header.Key, header.Value.ToString());
            }

            var result = new V4SignatureResult(
                DateTime.UtcNow.ToString(Constants.AmazonTimestampFormatCompact),
                s3ctx.Http.Request.Method,
                s3ctx.Http.Request.GetDisplayUrl(),
                s3ctx.Request.AccessKey,
                secretKey,
                s3ctx.Request.Region,
                "s3",
                headers,
                await GetRequestBody(s3ctx.Http.Request));

            if (!result.Signature.Equals(s3ctx.Request.Signature))
            {
                _logger.LogWarning("Invalid v4 signature '{Signature}'", s3ctx.Request.Signature);
                throw new S3Exception(new Error(ErrorCode.SignatureDoesNotMatch));
            }
        }

        private async Task<byte[]> GetRequestBody(HttpRequest request)
        {
            if (!request.Body.CanSeek)
            {
                // Create a seekable copy of the body
                var memoryStream = new MemoryStream();
                await request.Body.CopyToAsync(memoryStream);
                memoryStream.Position = 0;
                request.Body = memoryStream;
                return memoryStream.ToArray();
            }

            var buffer = new byte[request.ContentLength ?? 0];
            await request.Body.ReadAsync(buffer, 0, buffer.Length);
            request.Body.Position = 0;
            return buffer;
        }

        private async Task HandleS3Exception(HttpContext context, S3Context s3ctx, S3Exception s3e)
        {
            _logger.LogError(s3e, "S3 exception");

            if (s3ctx != null)
            {
                context.Response.StatusCode = s3e.HttpStatusCode;
                context.Response.ContentType = Constants.ContentTypeXml;
                await SendXmlResponse(context, s3e.Error);
            }
        }

        private async Task HandleGenericException(HttpContext context, S3Context s3ctx, Exception e)
        {
            _logger.LogError(e, "Unhandled exception");

            if (s3ctx != null)
            {
                context.Response.StatusCode = StatusCodes.Status500InternalServerError;
                context.Response.ContentType = Constants.ContentTypeXml;
                await SendErrorResponse(context, ErrorCode.InternalError);
            }
        }

        private async Task HandleRequestCompletion(S3Context s3ctx)
        {
            if (s3ctx != null)
            {
                if (_settings.PostRequestHandler != null)
                {
                    await _settings.PostRequestHandler(s3ctx);
                }
            }
        }

        private async Task SendResponse(HttpContext context, S3Response response)
        {
            if (response == null)
            {
                return;
            }

            foreach (string key in response.Headers.Keys)
            {
                context.Response.Headers[key] = response.Headers[key];
            }

            context.Response.StatusCode = response.StatusCode;
            context.Response.ContentType = response.ContentType;

            if (response.ContentLength > 0)
            {
                context.Response.ContentLength = response.ContentLength;
                await response.Data.CopyToAsync(context.Response.Body);
            }
        }

        private async Task SendXmlResponse<T>(HttpContext context, T obj)
        {
            var xml = SerializationHelper.SerializeXml(obj);
            await context.Response.WriteAsync(xml);
        }

        private async Task SendErrorResponse(HttpContext context, ErrorCode errorCode)
        {
            var error = new Error(errorCode);
            context.Response.StatusCode = error.HttpStatusCode;
            context.Response.ContentType = Constants.ContentTypeXml;
            await SendXmlResponse(context, error);
        }

        private void LogMessage(string message)
        {
            _logger.LogInformation(_header + message);
            _settings.Logger?.Invoke(_header + message);
        }
    }

    // Extension method for easy middleware registration
    public static class S3RequestHandlerExtensions
    {
        public static IApplicationBuilder UseS3Server(this IApplicationBuilder builder)
        {
            return builder.UseMiddleware<S3RequestHandler>();
        }
    }
}