package org.uche.t0k3nr.trader.kraken.util;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;


public class KrakenCredentials {

    private String key;

    private String secret;

    public KrakenCredentials(String key, String secret) {
    	this.key = key;
    	this.secret = secret;
    }
    
    public String sign(String urlPath, String nonce, String urlEncodedParams) {

        byte[] hmacKey = Base64.getDecoder().decode(secret);

        byte[] sha256 = sha256(nonce + urlEncodedParams);
        byte[] hmacMessage = concat(urlPath.getBytes(StandardCharsets.UTF_8), sha256);

        byte[] hmac = hmacSha512(hmacKey, hmacMessage);
        return Base64.getEncoder().encodeToString(hmac);
    }

    public static byte[] concat(byte[] a, byte[] b) {
        byte[] concat = new byte[a.length + b.length];
        System.arraycopy(a, 0, concat, 0, a.length);
        System.arraycopy(b, 0, concat, a.length, b.length);
        return concat;
    }

    public static byte[] hmacSha512(byte[] key, byte[] message) {
        try {
            Mac mac = Mac.getInstance("HmacSHA512");
            mac.init(new SecretKeySpec(key, "HmacSHA512"));
            return mac.doFinal(message);
        }
        catch (InvalidKeyException | NoSuchAlgorithmException e) {
            throw new IllegalStateException("Could not compute HMAC digest");
        }
    }

    private static byte[] sha256(String message) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(message.getBytes(StandardCharsets.UTF_8));
        }
        catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("Could not compute SHA-256 digest", e);
        }
    }

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getSecret() {
		return secret;
	}

	public void setSecret(String secret) {
		this.secret = secret;
	}
}
