/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.CryptUtils;
import org.elasticsearch.license.License;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

/**
 * Responsible for generating a license signature according to the signature spec and sign it with
 * the provided encrypted private key
 */
public class LicenseSigner {

    private static final int MAGIC_LENGTH = 13;
    private final Path publicKeyPath;
    private final Path privateKeyPath;

    public LicenseSigner(final Path privateKeyPath, final Path publicKeyPath) {
        this.publicKeyPath = publicKeyPath;
        this.privateKeyPath = privateKeyPath;
    }

    /**
     * Generates a signature for the {@code licenseSpec}. Signature structure:
     * <code>
     * | VERSION | MAGIC | PUB_KEY_DIGEST | SIGNED_LICENSE_CONTENT |
     * </code>
     *
     * @return a signed License
     */
    public License sign(License licenseSpec) throws IOException {
        XContentBuilder contentBuilder = XContentFactory.contentBuilder(XContentType.JSON);
        final Map<String, String> licenseSpecViewMode =
                Collections.singletonMap(License.LICENSE_SPEC_VIEW_MODE, "true");
        licenseSpec.toXContent(contentBuilder, new ToXContent.MapParams(licenseSpecViewMode));
        final byte[] signedContent;
        try {
            final Signature rsa = Signature.getInstance("SHA512withRSA");
            PrivateKey decryptedPrivateKey = CryptUtils.readEncryptedPrivateKey(Files.readAllBytes(privateKeyPath));
            rsa.initSign(decryptedPrivateKey);
            final BytesRefIterator iterator = BytesReference.bytes(contentBuilder).iterator();
            BytesRef ref;
            while((ref = iterator.next()) != null) {
                rsa.update(ref.bytes, ref.offset, ref.length);
            }
            signedContent = rsa.sign();
        } catch (InvalidKeyException
                | IOException
                | NoSuchAlgorithmException
                | SignatureException e) {
            throw new IllegalStateException(e);
        }
        final byte[] magic = new byte[MAGIC_LENGTH];
        SecureRandom random = new SecureRandom();
        random.nextBytes(magic);
        final byte[] publicKeyBytes = Files.readAllBytes(publicKeyPath);
        PublicKey publicKey = CryptUtils.readPublicKey(publicKeyBytes);
        final boolean preV4 = licenseSpec.version() < License.VERSION_CRYPTO_ALGORITHMS;

        if (preV4) {
            final byte[] pubKeyFingerprint = Base64.getEncoder().encode(CryptUtils.writeEncryptedPublicKey(publicKey));
            final String signature = createSignature(licenseSpec.version(), signedContent, magic, pubKeyFingerprint);

            return License.builder()
                .fromLicenseSpec(licenseSpec, signature, signature)
                .build();
        } else {
            final byte[] pubKeyFingerprintV3 = Base64.getEncoder().encode(CryptUtils.writeEncryptedPublicKey(publicKey));
            final String signatureV3 = createSignature(License.VERSION_START_DATE, signedContent, magic, pubKeyFingerprintV3);

            final byte[] pubKeyFingerprintV4 = getPublicKeyFingerprint(publicKeyBytes);
            final String signatureV4 = createSignature(licenseSpec.version(), signedContent, magic, pubKeyFingerprintV4);

            return License.builder()
                .fromLicenseSpec(licenseSpec, signatureV4, signatureV3)
                .build();
        }
    }

    private String createSignature(int version, byte[] signedContent, byte[] magic, byte[] pubKeyFingerprint) {
        byte[] bytes = new byte[4 + 4 + MAGIC_LENGTH + 4 + pubKeyFingerprint.length + 4 + signedContent.length];
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        byteBuffer.putInt(version)
            .putInt(magic.length)
            .put(magic)
            .putInt(pubKeyFingerprint.length)
            .put(pubKeyFingerprint)
            .putInt(signedContent.length)
            .put(signedContent);

        return Base64.getEncoder().encodeToString(bytes);
    }

    private byte[] getPublicKeyFingerprint(byte[] keyBytes) {
        MessageDigest sha256 = MessageDigests.sha256();
        sha256.update(keyBytes);
        return sha256.digest();
    }
}
