using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using S3ServerLibrary;
using S3ServerLibrary.S3Objects;
using S3ServerLibrary.Callbacks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Test.Server;

public class Program
{
    private static S3ServerSettings _Settings = new();
    private static bool _ForcePathStyle = true;
    private static bool _ValidateSignatures = false;
    private static bool _DebugSignatures = true;
    private static string _Location = "us-west-1";
    private static string _SecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    private static bool _RandomizeHeadResponses = false;
    private static Random _Random = new(int.MaxValue);

    // Shared objects for responses
    private static ObjectMetadata _ObjectMetadata =
        new("hello.txt", DateTime.Now, "etag", 13, new Owner("admin", "Administrator"));

    private static Owner _Owner = new("admin", "Administrator");
    private static Grantee _Grantee = new("admin", "Administrator", null, "CanonicalUser", "admin@admin.com");
    private static Tag _Tag = new("key", "value");

    public static async Task Main(string[] args)
    {
        var builder = WebApplication.CreateBuilder(args);

        // Configure services
        ConfigureServices(builder.Services);

        var app = builder.Build();

        // Configure middleware
        ConfigureMiddleware(app);

        // Start the application
        await app.RunAsync();
    }

    private static void ConfigureServices(IServiceCollection services)
    {
        // Configure S3 settings
        _Settings.Logging.HttpRequests = false;
        _Settings.Logging.S3Requests = false;
        _Settings.Logger = Logger;
        _Settings.EnableSignatures = _ValidateSignatures;

        if (_Settings.Logging.SignatureV4Validation && _DebugSignatures) _Settings.Logging.SignatureV4Validation = true;

        // Register services
        services.AddSingleton(_Settings);
        services.AddSingleton(CreateServiceCallbacks());
        services.AddSingleton(CreateBucketCallbacks());
        services.AddSingleton(CreateObjectCallbacks());

        // Add logging
        services.AddLogging(logging =>
        {
            logging.AddConsole();
            logging.SetMinimumLevel(LogLevel.Information);
        });
    }

    private static void ConfigureMiddleware(WebApplication app)
    {
        app.UseS3Server();

        // Add a health check endpoint
        app.MapGet("/health", () => "S3 Server is running!");
    }

    private static ServiceCallbacks CreateServiceCallbacks()
    {
        return new ServiceCallbacks
        {
            GetSecretKey = GetSecretKey,
            FindMatchingBaseDomain = _ForcePathStyle ? null : FindMatchingBaseDomain,
            ListBuckets = ListBuckets,
            ServiceExists = ServiceExists
        };
    }

    private static BucketCallbacks CreateBucketCallbacks()
    {
        return new BucketCallbacks
        {
            Delete = BucketDelete,
            DeleteTagging = BucketDeleteTags,
            DeleteWebsite = BucketDeleteWebsite,
            Exists = BucketExists,
            Read = BucketRead,
            ReadAcl = BucketReadAcl,
            ReadLocation = BucketReadLocation,
            ReadLogging = BucketReadLogging,
            ReadTagging = BucketReadTags,
            ReadVersioning = BucketReadVersioning,
            ReadVersions = BucketReadVersions,
            ReadWebsite = BucketReadWebsite,
            Write = BucketWrite,
            WriteAcl = BucketWriteAcl,
            WriteLogging = BucketWriteLogging,
            WriteTagging = BucketWriteTags,
            WriteVersioning = BucketWriteVersioning,
            WriteWebsite = BucketWriteWebsite
        };
    }

    private static ObjectCallbacks CreateObjectCallbacks()
    {
        return new ObjectCallbacks
        {
            Delete = ObjectDelete,
            DeleteMultiple = ObjectDeleteMultiple,
            DeleteTagging = ObjectDeleteTags,
            Exists = ObjectExists,
            Read = ObjectRead,
            ReadAcl = ObjectReadAcl,
            ReadLegalHold = ObjectReadLegalHold,
            ReadRetention = ObjectReadRetention,
            ReadRange = ObjectReadRange,
            ReadTagging = ObjectReadTags,
            Write = ObjectWrite,
            WriteAcl = ObjectWriteAcl,
            WriteLegalHold = ObjectWriteLegalHold,
            WriteRetention = ObjectWriteRetention,
            WriteTagging = ObjectWriteTags
        };
    }
    
    private static string GetSecretKey(S3Context context) => _SecretKey;

    private static string FindMatchingBaseDomain(string hostname)
    {
        if (string.IsNullOrEmpty(hostname)) throw new ArgumentNullException(nameof(hostname));

        if (hostname.Equals("s3.local.gd")) return "s3.local.gd";
        if (hostname.EndsWith(".s3.local.gd")) return "s3.local.gd";

        throw new KeyNotFoundException($"A base domain could not be found for hostname '{hostname}'.");
    }

    private static async Task<ListAllMyBucketsResult> ListBuckets(S3Context ctx)
    {
        Logger("ListBuckets");

        return new ListAllMyBucketsResult
        {
            Owner = new Owner("admin", "Administrator"),
            Buckets = new Buckets(new List<Bucket>
            {
                new Bucket("default", DateTime.Now)
            })
        };
    }

    private static async Task<string> ServiceExists(S3Context context) => "us-west-1";

    #region Bucket Operations

    private static async Task BucketDelete(S3Context ctx)
    {
        Logger($"BucketDelete: {ctx.Request.Bucket}");
    }

    private static async Task BucketDeleteTags(S3Context ctx)
    {
        Logger($"BucketDeleteTags: {ctx.Request.Bucket}");
    }

    private static async Task BucketDeleteWebsite(S3Context ctx)
    {
        Logger($"BucketDeleteWebsite: {ctx.Request.Bucket}");
    }

    private static async Task<bool> BucketExists(S3Context ctx)
    {
        Logger($"BucketExists: {ctx.Request.Bucket}");
        return !_RandomizeHeadResponses || _Random.Next(100) % 2 == 0;
    }

    private static async Task<ListBucketResult> BucketRead(S3Context ctx)
    {
        Logger($"BucketRead: {ctx.Request.Bucket}");

        return new ListBucketResult(
            "default",
            new List<ObjectMetadata> { _ObjectMetadata },
            1,
            ctx.Request.MaxKeys,
            ctx.Request.Prefix,
            ctx.Request.Marker,
            ctx.Request.Delimiter,
            false,
            null,
            null,
            _Location);
    }

    private static async Task<AccessControlPolicy> BucketReadAcl(S3Context ctx)
    {
        Logger($"BucketReadAcl: {ctx.Request.Bucket}");

        var acl = new AccessControlList(
            new List<Grant>
            {
                new Grant(_Grantee, PermissionEnum.FullControl)
            });

        return new AccessControlPolicy(_Owner, acl);
    }

    private static async Task<LocationConstraint> BucketReadLocation(S3Context ctx)
    {
        Logger($"BucketReadLocation: {ctx.Request.Bucket}");
        return new LocationConstraint(_Location);
    }

    private static async Task<BucketLoggingStatus> BucketReadLogging(S3Context ctx)
    {
        Logger($"BucketReadLogging: {ctx.Request.Bucket}");
        return new BucketLoggingStatus(
            new LoggingEnabled("default", "prefix", new TargetGrants()));
    }

    private static async Task<Tagging> BucketReadTags(S3Context ctx)
    {
        Logger($"BucketReadTags: {ctx.Request.Bucket}");
        return new Tagging(
            new TagSet(new List<Tag> { _Tag }));
    }

    private static async Task<VersioningConfiguration> BucketReadVersioning(S3Context ctx)
    {
        Logger($"BucketReadVersioning: {ctx.Request.Bucket}");
        return new VersioningConfiguration(
            VersioningStatusEnum.Enabled, 
            MfaDeleteStatusEnum.Disabled);
    }

    private static async Task<ListVersionsResult> BucketReadVersions(S3Context ctx)
    {
        Logger($"BucketReadVersions: {ctx.Request.Bucket}");

        var versions = new List<ObjectVersion>
        {
            new ObjectVersion(
                "version1.key", 
                "1", 
                true, 
                DateTime.UtcNow, 
                "etag", 
                500, 
                _Owner)
        };

        var deleteMarkers = new List<DeleteMarker>
        {
            new DeleteMarker(
                "deleted1.key", 
                "2", 
                true, 
                DateTime.UtcNow, 
                _Owner)
        };

        return new ListVersionsResult(
            "default",
            versions,
            deleteMarkers,
            ctx.Request.MaxKeys,
            ctx.Request.Prefix,
            ctx.Request.Marker,
            null,
            false,
            "us-west-1");
    }

    private static async Task<WebsiteConfiguration> BucketReadWebsite(S3Context ctx)
    {
        Logger($"BucketReadWebsite: {ctx.Request.Bucket}");

        return new WebsiteConfiguration
        {
            ErrorDocument = new ErrorDocument("error.html"),
            IndexDocument = new IndexDocument("index.html"),
            RedirectAllRequestsTo = new RedirectAllRequestsTo("localhost", ProtocolEnum.Http),
            RoutingRules = new RoutingRules(
                new List<RoutingRule>
                {
                    new RoutingRule(
                        new Condition("400", "prefix"),
                        new Redirect("localhost", 302, ProtocolEnum.Http, null, null))
                })
        };
    }

    private static async Task BucketWrite(S3Context ctx)
    {
        Logger($"BucketWrite: {ctx.Request.Bucket}");
    }

    private static async Task BucketWriteAcl(S3Context ctx, AccessControlPolicy acp)
    {
        Logger($"BucketWriteAcl: {ctx.Request.Bucket}");
        Logger(ctx.Request.DataAsString);
    }

    private static async Task BucketWriteLogging(S3Context ctx, BucketLoggingStatus logging)
    {
        Logger($"BucketWriteLogging: {ctx.Request.Bucket}");
        Logger(ctx.Request.DataAsString);
    }

    private static async Task BucketWriteTags(S3Context ctx, Tagging tags)
    {
        Logger($"BucketWriteTags: {ctx.Request.Bucket}");
        Logger(ctx.Request.DataAsString);
    }

    private static async Task BucketWriteVersioning(S3Context ctx, VersioningConfiguration ver)
    {
        Logger($"BucketWriteVersioning: {ctx.Request.Bucket}");
        Logger(ctx.Request.DataAsString);
    }

    private static async Task BucketWriteWebsite(S3Context ctx, WebsiteConfiguration website)
    {
        Logger($"BucketWriteWebsite: {ctx.Request.Bucket}");
        Logger(ctx.Request.DataAsString);
    }

    #endregion

    #region Object Operations

    private static async Task ObjectDelete(S3Context ctx)
    {
        Logger($"ObjectDelete: {ctx.Request.Bucket}/{ctx.Request.Key}");
    }

    private static async Task<DeleteResult> ObjectDeleteMultiple(S3Context ctx, DeleteMultiple del)
    {
        Logger($"ObjectDeleteMultiple: {ctx.Request.Bucket}");
        Logger(ctx.Request.DataAsString);

        return new DeleteResult(
            new List<Deleted> { new Deleted("hello.txt", "1", false) },
            null);
    }

    private static async Task<ObjectMetadata> ObjectExists(S3Context ctx)
    {
        Logger($"ObjectExists: {ctx.Request.Bucket}/{ctx.Request.Key}");
        return !_RandomizeHeadResponses || _Random.Next(100) % 2 == 0 ? _ObjectMetadata : null;
    }

    private static async Task<S3Object> ObjectRead(S3Context ctx)
    {
        Logger($"ObjectRead: {ctx.Request.Bucket}/{ctx.Request.Key}");

        return new S3Object(
            "hello.txt", 
            "1", 
            true, 
            DateTime.Now, 
            "etag", 
            13, 
            new Owner("admin", "Administrator"), 
            "Hello, world!", 
            "text/plain");
    }

    private static async Task ObjectDeleteTags(S3Context ctx)
    {
        Logger($"ObjectDeleteTags: {ctx.Request.Bucket}/{ctx.Request.Key}");
    }

    private static async Task<AccessControlPolicy> ObjectReadAcl(S3Context ctx)
    {
        Logger($"ObjectReadAcl: {ctx.Request.Bucket}/{ctx.Request.Key}");

        var acl = new AccessControlList(
            new List<Grant>
            {
                new Grant(_Grantee, PermissionEnum.FullControl)
            });

        return new AccessControlPolicy(_Owner, acl);
    }

    private static async Task<LegalHold> ObjectReadLegalHold(S3Context ctx)
    {
        Logger($"ObjectReadLegalHold: {ctx.Request.Bucket}/{ctx.Request.Key}");
        return new LegalHold("OFF");
    }

    private static async Task<S3Object> ObjectReadRange(S3Context ctx)
    {
        Logger($"ObjectReadRange: {ctx.Request.Bucket}/{ctx.Request.Key}");

        var s3obj = new S3Object(
            "hello.txt",
            "1",
            true,
            DateTime.Now,
            "etag",
            13,
            new Owner("admin", "Administrator"),
            "Hello, world!",
            "text/plain");

        string data = s3obj.DataString;
        data = data.Substring(
            (int)ctx.Request.RangeStart,
            (int)(ctx.Request.RangeEnd - ctx.Request.RangeStart));
        
        s3obj.DataString = data;
        s3obj.Size = data.Length;
        return s3obj;
    }

    private static async Task<Retention> ObjectReadRetention(S3Context ctx)
    {
        Logger($"ObjectReadRetention: {ctx.Request.Bucket}/{ctx.Request.Key}");
        return new Retention(
            RetentionModeEnum.Governance,
            DateTime.Now.AddDays(100));
    }

    private static async Task<Tagging> ObjectReadTags(S3Context ctx)
    {
        Logger($"ObjectReadTags: {ctx.Request.Bucket}/{ctx.Request.Key}");
        return new Tagging(
            new TagSet(new List<Tag> { _Tag }));
    }

    private static async Task ObjectWrite(S3Context ctx)
    {
        Logger($"ObjectWrite: {ctx.Request.Bucket}/{ctx.Request.Key}");
        Logger($"Content type: {ctx.Request.ContentType}");
        Logger($"Chunked transfer: {ctx.Request.Chunked}");

        if (ctx.Request.Chunked)
        {
            while (true)
            {
                var chunk = await ctx.Request.ReadChunk();
                using var buffer = chunk.buffer;
                var chunkLength = chunk.length;
                var chunkIsFinal = buffer.Memory.Length != chunkLength;
                Logger(SerializationHelper.SerializeJson(chunk, true));
                
                if (chunkLength > 0)
                {
                    Logger($"{chunkLength}/{chunkIsFinal}: {Encoding.UTF8.GetString(buffer.Memory.Span.Slice(0, chunkLength).ToArray())}");
                }

                if (chunkIsFinal)
                {
                    Logger("Final chunk encountered");
                    break;
                }
            }
        }
        else
        {
            Logger($"{ctx.Request.ContentLength}: {ctx.Request.DataAsString}");
        }
    }

    private static async Task ObjectWriteAcl(S3Context ctx, AccessControlPolicy acp)
    {
        Logger($"ObjectWriteAcl: {ctx.Request.Bucket}/{ctx.Request.Key}");
        Logger(ctx.Request.DataAsString);
    }

    private static async Task ObjectWriteLegalHold(S3Context ctx, LegalHold legalHold)
    {
        Logger($"ObjectWriteLegalHold: {ctx.Request.Bucket}/{ctx.Request.Key}");
        Logger(ctx.Request.DataAsString);
    }

    private static async Task ObjectWriteRetention(S3Context ctx, Retention retention)
    {
        Logger($"ObjectWriteRetention: {ctx.Request.Bucket}/{ctx.Request.Key}");
        Logger(ctx.Request.DataAsString);
    }

    private static async Task ObjectWriteTags(S3Context ctx, Tagging tags)
    {
        Logger($"ObjectWriteTags: {ctx.Request.Bucket}/{ctx.Request.Key}");
        Logger(ctx.Request.DataAsString);
    }
#endregion

    #region Misc

    private static void Logger(string msg)
    {
        Console.WriteLine(msg);
    }

    #endregion

#pragma warning restore CS1998 // Async method lacks 'await' operators and will run synchronously
}