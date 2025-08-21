from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, unbase64, base64, aes_encrypt, aes_decrypt, when, length
from pyspark.sql.types import BinaryType
import base64 as py_base64

class EncryptionHelper:
    def __init__(self, dbutils):
        self.dbutils = dbutils
        secret_key = dbutils.secrets.get(scope='secret-scope', key='encryption-key')
        self.encryption_key = py_base64.b64decode(secret_key)
        self.aes_mode = 'ECB'
        self.aes_padding = 'PKCS'

    def encrypt_dataframe(self, df: DataFrame, columns_to_encrypt: list) -> DataFrame:
        for column in columns_to_encrypt:
            try:
                df = df.withColumn(
                    column,
                    base64(aes_encrypt(col(column).cast(BinaryType()), lit(self.encryption_key), lit(self.aes_mode), lit(self.aes_padding)))
                )
            except Exception as e:
                print(f"Exception thrown while encrypting column '{column}': {e}")
        return df

    def decrypt_dataframe(self, df: DataFrame, columns_to_decrypt: list) -> DataFrame:
        b64 = "^[A-Za-z0-9+/]*={0,2}$"
        out = df
        for c in columns_to_decrypt:
            looks_b64 = (
                col(c).isNotNull() &
                (length(col(c)) > 0) &
                ((length(col(c)) % 4) == 0) &
                col(c).rlike(b64)
            )
            out = out.withColumn(
                c,
                when(
                    looks_b64,
                    aes_decrypt(
                        unbase64(col(c)),
                        lit(self.encryption_key),
                        lit(self.aes_mode),
                        lit(self.aes_padding)
                    ).cast('string')
                ).otherwise(col(c))
            )
        return out