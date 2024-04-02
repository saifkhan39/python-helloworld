from cryptography.fernet import Fernet

def decrypt(token: str, key: str) -> str:
    return Fernet(key.encode()).decrypt(token.encode()).decode()