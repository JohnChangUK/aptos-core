// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

//! This file implements traits for Ed25519 private keys and public keys.

#[cfg(any(test, feature = "fuzzing"))]
use crate::test_utils::{self, KeyPair};
use crate::{
    p256::{P256Signature, P256_PRIVATE_KEY_LENGTH, P256_PUBLIC_KEY_LENGTH},
    hash::CryptoHash,
    traits::*,
};
use openssl::{nid::Nid, ec::EcKey, ec::EcGroup, ecdsa::EcdsaSig, pkey::Private, pkey::Public};
use aptos_crypto_derive::{DeserializeKey, SerializeKey, SilentDebug, SilentDisplay};
use core::convert::TryFrom;
#[cfg(any(test, feature = "fuzzing"))]
use proptest::prelude::*;
use serde::Serialize;
use std::fmt;

/// A P256 private key
#[derive(DeserializeKey, SerializeKey, SilentDebug, SilentDisplay)]
pub struct P256PrivateKey(pub(crate) EcKey<Private>);

impl private::Sealed for P256PrivateKey {}

#[cfg(feature = "assert-private-keys-not-cloneable")]
static_assertions::assert_not_impl_any!(P256PrivateKey: Clone);

#[cfg(any(test, feature = "cloneable-private-keys"))]
impl Clone for P256PrivateKey {
    fn clone(&self) -> Self {
        let serialized: &[u8] = &(self.to_bytes());
        P256PrivateKey::try_from(serialized).unwrap()
    }
}

/// A P256 public key
#[derive(DeserializeKey, Clone, SerializeKey)]
pub struct P256PublicKey(pub(crate) EcKey<Public>);

impl private::Sealed for P256PublicKey {}

impl P256PrivateKey {
    /// The length of the P256PrivateKey
    pub const LENGTH: usize = P256_PRIVATE_KEY_LENGTH;

    /// Serialize a P256PrivateKey.
    pub fn to_bytes(&self) -> [u8; P256_PRIVATE_KEY_LENGTH] {
        // TODO: Error handling
        let test = self.0.private_key_to_der().unwrap();
        // TODO: Error handling
        test[..].try_into().unwrap()
    }

    /// Deserialize an P256PrivateKey without any validation checks apart from expected key size.
    fn from_bytes_unchecked(
        bytes: &[u8],
    ) -> std::result::Result<P256PrivateKey, CryptoMaterialError> {
        match EcKey::private_key_from_der(&bytes) {
            Ok(p256_secret_key) => Ok(P256PrivateKey(p256_secret_key)),
            Err(_) => Err(CryptoMaterialError::DeserializationError),
        }
    }

    /// Private function aimed at minimizing code duplication between sign
    /// methods of the SigningKey implementation. This should remain private.
    fn sign_arbitrary_message(&self, message: &[u8]) -> P256Signature {
        // TODO: Fix error handling
        let secret_key = &self.0;
        // TODO: Fix error handling
        let sig = EcdsaSig::sign(message, &secret_key).unwrap();
        P256Signature(sig)
    }
}

impl P256PublicKey {
    /// Serialize a P256PublicKey.
    // TODO: Better error handling here. Also should we compress?
    pub fn to_bytes(&self) -> [u8; P256_PUBLIC_KEY_LENGTH] {
        // TODO: Error handling
        let bytes = self.0.public_key_to_der().unwrap();
        // TODO: Error handling
        bytes.try_into().unwrap()
    }

    /// Deserialize a P256PublicKey without any validation checks apart from expected key size
    /// and valid curve point, although not necessarily in the prime-order subgroup.
    ///
    /// This function does NOT check the public key for membership in a small subgroup.
    pub(crate) fn from_bytes_unchecked(
        bytes: &[u8],
    ) -> std::result::Result<P256PublicKey, CryptoMaterialError> {
        match EcKey::public_key_from_der(&bytes) {
            Ok(p256_public_key) => Ok(P256PublicKey(p256_public_key)),
            Err(_) => Err(CryptoMaterialError::DeserializationError),
        }
    }
}

///////////////////////
// PrivateKey Traits //
///////////////////////

impl PrivateKey for P256PrivateKey {
    type PublicKeyMaterial = P256PublicKey;
}

impl SigningKey for P256PrivateKey {
    type SignatureMaterial = P256Signature;
    type VerifyingKeyMaterial = P256PublicKey;

    fn sign<T: CryptoHash + Serialize>(
        &self,
        message: &T,
    ) -> Result<P256Signature, CryptoMaterialError> {
        Ok(P256PrivateKey::sign_arbitrary_message(
            self,
            signing_message(message)?.as_ref(),
        ))
    }

    #[cfg(any(test, feature = "fuzzing"))]
    fn sign_arbitrary_message(&self, message: &[u8]) -> P256Signature {
        P256PrivateKey::sign_arbitrary_message(self, message)
    }
}

impl Uniform for P256PrivateKey {
    // TODO: Remove rng
    fn generate<R>(_rng: &mut R) -> Self
    where
        R: ::rand::RngCore + ::rand::CryptoRng + ::rand_core::CryptoRng + ::rand_core::RngCore,
    {
        let group: EcGroup = EcGroup::from_curve_name(Nid::X9_62_PRIME256V1).unwrap();
        // TODO: Make sure this is uniformly random
        let private_key: EcKey<Private> = EcKey::generate(&group).unwrap();
        P256PrivateKey(private_key)
    }
}

impl PartialEq<Self> for P256PrivateKey {
    fn eq(&self, other: &Self) -> bool {
        self.to_bytes() == other.to_bytes()
    }
}

impl Eq for P256PrivateKey {}

// We could have a distinct kind of validation for the PrivateKey: e.g., checking the derived
// PublicKey is valid?
impl TryFrom<&[u8]> for P256PrivateKey {
    type Error = CryptoMaterialError;

    /// Deserialize a P256PrivateKey. This method will check for private key validity: i.e.,
    /// correct key length.
    fn try_from(bytes: &[u8]) -> std::result::Result<P256PrivateKey, CryptoMaterialError> {
        // TODO: Check this comment for p256
        // Note that the only requirement is that the size of the key is 32 bytes, something that
        // is already checked during deserialization of ed25519_dalek::SecretKey
        //
        // Also, the underlying ed25519_dalek implementation ensures that the derived public key
        // is safe and it will not lie in a small-order group, thus no extra check for PublicKey
        // validation is required.
        P256PrivateKey::from_bytes_unchecked(bytes)
    }
}

impl Length for P256PrivateKey {
    fn length(&self) -> usize {
        Self::LENGTH
    }
}

impl ValidCryptoMaterial for P256PrivateKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_bytes().to_vec()
    }
}

impl Genesis for P256PrivateKey {
    fn genesis() -> Self {
        let mut buf = [0u8; P256_PRIVATE_KEY_LENGTH];
        buf[P256_PRIVATE_KEY_LENGTH - 1] = 1;
        Self::try_from(buf.as_ref()).unwrap()
    }
}

//////////////////////
// PublicKey Traits //
//////////////////////

// Implementing From<&PrivateKey<...>> allows to derive a public key in a more elegant fashion
impl From<&P256PrivateKey> for P256PublicKey {
    fn from(private_key: &P256PrivateKey) -> Self {
        let nid = Nid::X9_62_PRIME256V1; // NIST P-256 curve
        // TODO: Fix error handling
        let group = EcGroup::from_curve_name(nid).unwrap();
        let secret = &private_key.0;
        // TODO: Fix error handling
        let public = EcKey::from_public_key(&group, secret.public_key()).unwrap();
        P256PublicKey(public)
    }
}

// We deduce PublicKey from this
impl PublicKey for P256PublicKey {
    type PrivateKeyMaterial = P256PrivateKey;
}

impl std::hash::Hash for P256PublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let encoded_pubkey = self.to_bytes();
        state.write(&encoded_pubkey);
    }
}

// Those are required by the implementation of hash above
impl PartialEq for P256PublicKey {
    fn eq(&self, other: &P256PublicKey) -> bool {
        self.to_bytes() == other.to_bytes()
    }
}

impl Eq for P256PublicKey {}

// We deduce VerifyingKey from pointing to the signature material
// we get the ability to do `pubkey.validate(msg, signature)`
impl VerifyingKey for P256PublicKey {
    type SignatureMaterial = P256Signature;
    type SigningKeyMaterial = P256PrivateKey;
}

impl fmt::Display for P256PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: Error handling
        write!(f, "{}", hex::encode(self.0.public_key_to_der().unwrap()))
    }
}

impl fmt::Debug for P256PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "P256PublicKey({})", self)
    }
}

impl TryFrom<&[u8]> for P256PublicKey {
    type Error = CryptoMaterialError;

    /// Deserialize a P256PublicKey. This method will NOT check for key validity, which means
    /// the returned public key could be in a small subgroup. Nonetheless, our signature
    /// verification implicitly checks if the public key lies in a small subgroup, so canonical
    /// uses of this library will not be susceptible to small subgroup attacks.
    fn try_from(bytes: &[u8]) -> std::result::Result<P256PublicKey, CryptoMaterialError> {
        P256PublicKey::from_bytes_unchecked(bytes)
    }
}

impl Length for P256PublicKey {
    fn length(&self) -> usize {
        P256_PUBLIC_KEY_LENGTH
    }
}

impl ValidCryptoMaterial for P256PublicKey {
    fn to_bytes(&self) -> Vec<u8> {
        // TODO: Error handling
        self.0.public_key_to_der().unwrap()
    }
}

/////////////
// Fuzzing //
/////////////

/// Produces a uniformly random P256 keypair from a seed
#[cfg(any(test, feature = "fuzzing"))]
pub fn keypair_strategy() -> impl Strategy<Value = KeyPair<P256PrivateKey, P256PublicKey>> {
    test_utils::uniform_keypair_strategy::<P256PrivateKey, P256PublicKey>()
}

/// Produces a uniformly random P256 public key
#[cfg(any(test, feature = "fuzzing"))]
impl proptest::arbitrary::Arbitrary for P256PublicKey {
    type Parameters = ();
    type Strategy = BoxedStrategy<Self>;

    fn arbitrary_with(_args: Self::Parameters) -> Self::Strategy {
        crate::test_utils::uniform_keypair_strategy::<P256PrivateKey, P256PublicKey>()
            .prop_map(|v| v.public_key)
            .boxed()
    }
}