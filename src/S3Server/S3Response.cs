using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Specialized;
using System.Globalization;
using System.IO;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace S3ServerLibrary
{
    using S3ServerLibrary.S3Objects;

    /// <summary>
    /// S3 response wrapper for ASP.NET Core HttpResponse.
    /// </summary>
    public class S3Response
    {
        #region Public-Members

        /// <summary>
        /// The HTTP status code to return to the requestor (client).
        /// </summary>
        public int StatusCode
        {
            get => _httpResponse.StatusCode;
            set => _httpResponse.StatusCode = value;
        }

        /// <summary>
        /// User-supplied headers to include in the response.
        /// </summary>
        public NameValueCollection Headers
        {
            get
            {
                var headers = new NameValueCollection(StringComparer.InvariantCultureIgnoreCase);
                foreach (var header in _httpResponse.Headers)
                {
                    headers.Add(header.Key, header.Value.ToString());
                }
                return headers;
            }
            set
            {
                _httpResponse.Headers.Clear();
                if (value != null)
                {
                    foreach (string key in value.Keys)
                    {
                        _httpResponse.Headers[key] = value[key];
                    }
                }
            }
        }

        /// <summary>
        /// User-supplied content-type to include in the response.
        /// </summary>
        public string ContentType
        {
            get => _httpResponse.ContentType;
            set => _httpResponse.ContentType = value;
        }

        /// <summary>
        /// The length of the data in the response stream.
        /// </summary>
        public long ContentLength
        {
            get => _httpResponse.ContentLength ?? 0;
            set
            {
                if (value < 0) throw new ArgumentException("Content length must be zero or greater.");
                _httpResponse.ContentLength = value;
            }
        }

        /// <summary>
        /// Enable or disable chunked transfer-encoding.
        /// </summary>
        public bool ChunkedTransfer
        {
            get => string.Equals(_httpResponse.Headers["Transfer-Encoding"], "chunked", StringComparison.OrdinalIgnoreCase);
            set
            {
                if (value)
                    _httpResponse.Headers["Transfer-Encoding"] = "chunked";
                else
                    _httpResponse.Headers.Remove("Transfer-Encoding");
            }
        }

        /// <summary>
        /// The response body stream.
        /// </summary>
        [JsonIgnore]
        public Stream Data => _httpResponse.Body;

        /// <summary>
        /// Data stream as a string. Fully reads the data stream.
        /// </summary>
        [JsonIgnore]
        public string DataAsString
        {
            get
            {
                if (_httpResponse.Body.CanSeek)
                    _httpResponse.Body.Seek(0, SeekOrigin.Begin);

                using var reader = new StreamReader(_httpResponse.Body, Encoding.UTF8);
                return reader.ReadToEnd();
            }
        }

        /// <summary>
        /// Data stream as a byte array. Fully reads the data stream.
        /// </summary>
        [JsonIgnore]
        public byte[] DataAsBytes
        {
            get
            {
                if (_httpResponse.Body.CanSeek)
                    _httpResponse.Body.Seek(0, SeekOrigin.Begin);

                using var memoryStream = new MemoryStream();
                _httpResponse.Body.CopyTo(memoryStream);
                return memoryStream.ToArray();
            }
        }

        #endregion

        #region Private-Members

        private readonly HttpResponse _httpResponse;
        private readonly S3Request _s3Request;

        #endregion

        #region Constructors-and-Factories

        /// <summary>
        /// Instantiate.
        /// </summary>
        public S3Response()
        {
        }

        /// <summary>
        /// Instantiate the object without supplying a stream. Useful for HEAD responses.
        /// </summary>
        /// <param name="ctx">S3 context.</param>
        public S3Response(S3Context ctx)
        {
            if (ctx == null) throw new ArgumentNullException(nameof(ctx));

            _httpResponse = ctx.Http.Response;
            _s3Request = ctx.Request;
        }

        #endregion

        #region Public-Methods

        /// <summary>
        /// Send the response with no data to the requestor.
        /// </summary>
        /// <returns>True if successful.</returns>
        public async Task<bool> Send()
        {
            if (ChunkedTransfer)
                throw new IOException("Responses with chunked transfer-encoding enabled require use of SendChunk() and SendFinalChunk().");

            SetResponseHeaders();
            await _httpResponse.CompleteAsync();
            return true;
        }

        /// <summary>
        /// Send the response with the supplied data to the requestor.
        /// </summary>
        /// <param name="data">Data string.</param>
        /// <returns>True if successful.</returns>
        public async Task<bool> Send(string data)
        {
            if (ChunkedTransfer)
                throw new IOException("Responses with chunked transfer-encoding enabled require use of SendChunk() and SendFinalChunk().");

            byte[] bytes = string.IsNullOrEmpty(data) ? Array.Empty<byte>() : Encoding.UTF8.GetBytes(data);
            ContentLength = bytes.Length;
            
            SetResponseHeaders();
            await _httpResponse.Body.WriteAsync(bytes, 0, bytes.Length);
            await _httpResponse.CompleteAsync();
            return true;
        }

        /// <summary>
        /// Send the response with the supplied data to the requestor.
        /// </summary>
        /// <param name="data">Data bytes.</param>
        /// <returns>True if successful.</returns>
        public async Task<bool> Send(byte[] data)
        {
            if (ChunkedTransfer)
                throw new IOException("Responses with chunked transfer-encoding enabled require use of SendChunk() and SendFinalChunk().");

            if (data != null && data.Length > 0)
            {
                ContentLength = data.Length;
                SetResponseHeaders();
                await _httpResponse.Body.WriteAsync(data, 0, data.Length);
            }
            else
            {
                ContentLength = 0;
                SetResponseHeaders();
            }

            await _httpResponse.CompleteAsync();
            return true;
        }

        /// <summary>
        /// Send the response with the supplied stream to the requestor.
        /// </summary>
        /// <param name="contentLength">Content length.</param>
        /// <param name="stream">Stream containing data.</param>
        /// <returns>True if successful.</returns>
        public async Task<bool> Send(long contentLength, Stream stream)
        {
            if (ChunkedTransfer)
                throw new IOException("Responses with chunked transfer-encoding enabled require use of SendChunk() and SendFinalChunk().");

            ContentLength = contentLength;
            SetResponseHeaders();

            if (stream != null && ContentLength > 0)
            {
                await stream.CopyToAsync(_httpResponse.Body);
            }

            await _httpResponse.CompleteAsync();
            return true;
        }

        /// <summary>
        /// Send an error response to the requestor.
        /// </summary>
        /// <param name="error">Error object.</param>
        /// <returns>True if successful.</returns>
        public async Task<bool> Send(Error error)
        {
            ChunkedTransfer = false;

            byte[] bytes = Encoding.UTF8.GetBytes(SerializationHelper.SerializeXml(error));
            ContentLength = bytes.Length;
            StatusCode = error.HttpStatusCode;
            ContentType = Constants.ContentTypeXml;

            SetResponseHeaders();
            await _httpResponse.Body.WriteAsync(bytes, 0, bytes.Length);
            await _httpResponse.CompleteAsync();
            return true;
        }

        /// <summary>
        /// Send an error response to the requestor.
        /// </summary>
        /// <param name="error">ErrorCode.</param>
        /// <returns>True if successful.</returns>
        public async Task<bool> Send(ErrorCode error)
        {
            var errorBody = new Error(error);
            return await Send(errorBody);
        }

        /// <summary>
        /// Send a chunk of data using chunked transfer-encoding to the requestor.
        /// </summary>
        /// <param name="data">Chunk of data.</param>
        /// <param name="isFinal">Boolean indicating if the chunk is the final chunk.</param>
        /// <returns>True if successful.</returns>
        public async Task<bool> SendChunk(byte[] data, bool isFinal)
        {
            if (!ChunkedTransfer)
                throw new IOException("Responses with chunked transfer-encoding disabled require use of Send().");

            SetResponseHeaders();

            if (data != null && data.Length > 0)
            {
                var chunkSize = data.Length.ToString("X", CultureInfo.InvariantCulture);
                var chunkHeader = Encoding.ASCII.GetBytes($"{chunkSize}\r\n");
                await _httpResponse.Body.WriteAsync(chunkHeader, 0, chunkHeader.Length);
                await _httpResponse.Body.WriteAsync(data, 0, data.Length);
                await _httpResponse.Body.WriteAsync(Encoding.ASCII.GetBytes("\r\n"), 0, 2);
            }

            if (isFinal)
            {
                await _httpResponse.Body.WriteAsync(Encoding.ASCII.GetBytes("0\r\n\r\n"), 0, 5);
                await _httpResponse.CompleteAsync();
            }

            return true;
        }

        #endregion

        #region Private-Methods

        private void SetResponseHeaders()
        {
            if (!_httpResponse.Headers.ContainsKey("X-Amz-Date"))
                _httpResponse.Headers.Append("X-Amz-Date", DateTime.UtcNow.ToString(Constants.AmazonTimestampFormatVerbose, CultureInfo.InvariantCulture));

            if (!_httpResponse.Headers.ContainsKey("Host"))
                _httpResponse.Headers.Append("Host", _s3Request.Hostname);

            if (!_httpResponse.Headers.ContainsKey("Server"))
                _httpResponse.Headers.Append("Server", "S3Server");

            _httpResponse.Headers.Remove("Date");
            _httpResponse.Headers.Append("Date", DateTime.UtcNow.ToString(Constants.AmazonTimestampFormatVerbose, CultureInfo.InvariantCulture));
        }

        #endregion
    }
}