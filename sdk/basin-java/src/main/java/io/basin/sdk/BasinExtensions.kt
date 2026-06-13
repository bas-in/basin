@file:Suppress("EXTENSION_SHADOWED_BY_MEMBER", "unused")

package io.basin.sdk

import com.fasterxml.jackson.databind.JsonNode
import kotlinx.coroutines.future.await

/**
 * Kotlin suspend-function wrappers for {@link BasinClient}.
 *
 * These thin wrappers convert the SDK's {@link java.util.concurrent.CompletableFuture}
 * API into Kotlin coroutine-friendly {@code suspend fun}, making the SDK pleasant from
 * Kotlin without pulling in Reactive Streams or other heavy machinery.
 *
 * Add the coroutines dependency to use these:
 * ```kotlin
 * implementation("org.jetbrains.kotlinx:kotlinx-coroutines-jdk8:1.8.1")
 * ```
 *
 * Usage:
 * ```kotlin
 * val client = BasinClient.builder()
 *     .url("https://your-project.basin.run")
 *     .apiKey("your-api-key")
 *     .build()
 *
 * // In a coroutine:
 * val result = client.table("orders").select("id,total").awaitExecute()
 * val bucket  = client.storage.awaitCreateBucket("avatars", public = true)
 * val session = client.auth.awaitSignIn("a@b.com", "secret", null)
 * ```
 */

// ---------------------------------------------------------------------------
// QueryBuilder
// ---------------------------------------------------------------------------

/** Suspend variant of [QueryBuilder.execute]. */
suspend fun QueryBuilder.awaitExecute(): QueryResult = execute().await()

/** Suspend variant of insert. */
suspend fun QueryBuilder.awaitInsert(rows: Any): JsonNode = insert(rows).await()

/** Suspend variant of update. */
suspend fun QueryBuilder.awaitUpdate(values: Map<String, Any>): JsonNode = update(values).await()

/** Suspend variant of delete. */
suspend fun QueryBuilder.awaitDelete(): JsonNode = delete().await()

// ---------------------------------------------------------------------------
// AuthClient
// ---------------------------------------------------------------------------

/** Suspend variant of [AuthClient.signIn]. */
suspend fun AuthClient.awaitSignIn(email: String, password: String, projectId: String?): Session =
    signInAsync(email, password, projectId).await()

/** Suspend variant of [AuthClient.signUp]. */
suspend fun AuthClient.awaitSignUp(email: String, password: String, projectId: String?): SignUpResult =
    signUpAsync(email, password, projectId).await()

/** Suspend variant of [AuthClient.signOut]. */
suspend fun AuthClient.awaitSignOut(): Unit = signOutAsync().await()

/** Suspend variant of [AuthClient.refreshSession]. */
suspend fun AuthClient.awaitRefreshSession(): Session = refreshSessionAsync().await()

/** Suspend variant of [AuthClient.createApiKey]. */
suspend fun AuthClient.awaitCreateApiKey(name: String): ApiKeyIssued = createApiKeyAsync(name).await()

/** Suspend variant of [AuthClient.listApiKeys]. */
suspend fun AuthClient.awaitListApiKeys(): List<ApiKeyDescriptor> = listApiKeysAsync().await()

/** Suspend variant of [AuthClient.deleteApiKey]. */
suspend fun AuthClient.awaitDeleteApiKey(keyId: Long): JsonNode = deleteApiKeyAsync(keyId).await()

/** Suspend variant of [AuthClient.getOAuthAuthorizeUrl]. */
suspend fun AuthClient.awaitGetOAuthAuthorizeUrl(
    provider: String, redirectTo: String?, projectId: String?
): OAuthAuthorizeResult = getOAuthAuthorizeUrlAsync(provider, redirectTo, projectId).await()

/** Suspend variant of [AuthClient.enrollFactor]. */
suspend fun AuthClient.awaitEnrollFactor(factorType: String, friendlyName: String?): Any =
    enrollFactorAsync(factorType, friendlyName).await()

/** Suspend variant of [AuthClient.verifyFactor]. */
suspend fun AuthClient.awaitVerifyFactor(
    factorId: String, code: String?, attestation: String?, challengeId: String?
): VerifyFactorResult = verifyFactorAsync(factorId, code, attestation, challengeId).await()

/** Suspend variant of [AuthClient.challengeFactor]. */
suspend fun AuthClient.awaitChallengeFactor(factorId: String): Any =
    challengeFactorAsync(factorId).await()

/** Suspend variant of [AuthClient.verifyChallenge]. */
suspend fun AuthClient.awaitVerifyChallenge(
    factorId: String, challengeId: String, code: String?, assertion: String?
): Session = verifyChallengeAsync(factorId, challengeId, code, assertion).await()

/** Suspend variant of [AuthClient.unenrollFactor]. */
suspend fun AuthClient.awaitUnenrollFactor(factorId: String): JsonNode =
    unenrollFactorAsync(factorId).await()

// ---------------------------------------------------------------------------
// StorageClient
// ---------------------------------------------------------------------------

/** Suspend variant of [StorageClient.createBucket]. */
suspend fun StorageClient.awaitCreateBucket(
    name: String,
    public: Boolean = false,
    fileSizeLimit: Long? = null,
    allowedMimeTypes: List<String>? = null
): StorageBucket = createBucket(name, public, fileSizeLimit, allowedMimeTypes).await()

/** Suspend variant of [StorageClient.getBucket]. */
suspend fun StorageClient.awaitGetBucket(name: String): StorageBucket = getBucket(name).await()

/** Suspend variant of [StorageClient.deleteBucket]. */
suspend fun StorageClient.awaitDeleteBucket(name: String): JsonNode = deleteBucket(name).await()

// ---------------------------------------------------------------------------
// StorageBucketClient
// ---------------------------------------------------------------------------

/** Suspend variant of [StorageBucketClient.upload]. */
suspend fun StorageBucketClient.awaitUpload(
    path: String, data: ByteArray, contentType: String? = null
): StorageObject = upload(path, data, contentType).await()

/** Suspend variant of [StorageBucketClient.download]. */
suspend fun StorageBucketClient.awaitDownload(path: String): DownloadResult = download(path).await()

/** Suspend variant of [StorageBucketClient.remove]. */
suspend fun StorageBucketClient.awaitRemove(path: String): JsonNode = remove(path).await()

/** Suspend variant of [StorageBucketClient.list]. */
suspend fun StorageBucketClient.awaitList(
    prefix: String? = null, limit: Int? = null, offset: Int? = null
): List<StorageObject> = list(prefix, limit, offset).await()

/** Suspend variant of [StorageBucketClient.createSignedUrl]. */
suspend fun StorageBucketClient.awaitCreateSignedUrl(
    path: String, expiresIn: Int = 3600
): SignedUrl = createSignedUrl(path, expiresIn).await()

// ---------------------------------------------------------------------------
// FunctionsClient
// ---------------------------------------------------------------------------

/** Suspend variant of [FunctionsClient.invoke]. */
suspend fun FunctionsClient.awaitInvoke(
    name: String,
    method: String = "POST",
    body: Any? = null,
    headers: Map<String, String>? = null
): FunctionInvokeResult = invoke(name, method, body, headers).await()

/** Suspend variant of [FunctionsClient.rpc]. */
suspend fun FunctionsClient.awaitRpc(
    fnName: String, args: Map<String, Any>? = null
): JsonNode = rpc(fnName, args).await()
