/// Official Dart / Flutter client for Basin.
///
/// ```dart
/// import 'package:basin_sdk/basin_sdk.dart';
/// ```
library basin_sdk;

export 'src/auth.dart' show AuthClient, projectIdFromJwt;
export 'src/client.dart' show BasinClient;
export 'src/errors.dart'
    show BasinApiError, BasinError, BasinNetworkError, kErrorCodes;
export 'src/functions.dart' show FunctionsClient;
export 'src/query.dart' show QueryBuilder, QueryResult;
export 'src/realtime.dart' show ChannelHandle, RealtimeClient;
export 'src/storage.dart' show StorageBucketClient, StorageClient;
export 'src/types.dart'
    show
        ApiKeyDescriptor,
        ApiKeyIssued,
        Bucket,
        ChangeOp,
        DownloadResult,
        ExecTag,
        FactorDescriptor,
        FunctionInvokeResult,
        MfaChallengeResult,
        MfaEnrollResult,
        OAuthAuthorizeResult,
        Page,
        PresenceDiffFrame,
        PresenceEntry,
        PresenceErrorFrame,
        PresenceStateFrame,
        RealtimeErrorFrame,
        RealtimeEvent,
        RealtimeGapFrame,
        RealtimeServerFrame,
        Row,
        Session,
        SignedUrl,
        SignUpResult,
        StorageObject,
        TotpChallengeResult,
        TotpEnrollResult,
        VerifyFactorResult,
        WebAuthnChallengeResult,
        WebAuthnEnrollResult;
