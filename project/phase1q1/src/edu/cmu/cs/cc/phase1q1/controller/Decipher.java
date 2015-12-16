package edu.cmu.cs.cc.phase1q1.controller;

import java.math.BigInteger;

public class Decipher {
	private static final BigInteger X = new BigInteger("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773");
//	public static void main(String[] args) {
//		String cipherText = "RLZFJDFHJJDTAYBZBBZUZHIWRVVTANCPGDAN";
//		String key = "8";
//		System.out.println(deciph(cipherText, key));
//		String message = "JDXRZSXBQRBBTMJVVRRNULTZNHTALYOSVFSF";
//		int key2 = 8;
//		String ciphered = ciph(message, key2);
//		System.out.println(ciphered);
//	}

	public static String ciph(String message, int key) {
		int length = message.length();
		int size = (int) Math.sqrt(length);
		StringBuilder sb = new StringBuilder();
		int[] leftMost = new int[size];
		for (int i = 0; i < size; i++) {
			if (i == 0) {
				leftMost[i] = 0;
			} else {
				leftMost[i] += i + 1 + leftMost[i - 1];
			}
		}
		
		for (int i = 0; i < size; i++) {
			int base = leftMost[i];
			boolean firstEnd = false;
			boolean secondEnd = false;
			for (int j = 0; j < size; j++) {
				char c = (char) (message.charAt(base) + key);
				if (c > 'Z') {
					c -= 26;
				}
				sb.append(c);
				if (!secondEnd && j + i + 1 > size - 1) {
					base += size - 1;
				} else if (!secondEnd) {
					base += j + i + 1;
				} else if (secondEnd) {
					base += size - j + size - i - 1;
				}
				if (!firstEnd && j + i + 1 > size - 1) {
					firstEnd = true;
				} else if (firstEnd && j + i + 1 > size - 1) {
					secondEnd = true;
				}
				
				if (secondEnd) {
					base--;
				}
				
			}
		}
		return sb.toString();
	}

	public static String deciph(String cipherText, String rawKey) {
		BigInteger bi = new BigInteger(rawKey);
		BigInteger Y =  bi.divide(X);
		
		BigInteger bigKey = Y.mod(BigInteger.valueOf(25)).add(BigInteger.ONE);
		int key = bigKey.intValue();
		
		int length = cipherText.length();
		int size = (int) Math.sqrt(length);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 2 * size - 1; i++) {
			int z1 = i < size ? 0 : i - size + 1;
			int z2 = i < size ? 0 : i - size + 1;
			for (int j = i - z2; j >= z1; j--) {
				char c = (char) (cipherText.charAt(j + (i - j) * size) - key);
				if (c < 'A') {
					c += 26;
				}
				sb.append(c);
			}
		}
		return sb.toString();
	}
}
