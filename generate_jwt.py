import time
import jwt
import json


def generate_jwt(signing_key, client_id) -> str:

    payload = {
        # Issued at time
        "iat": int(time.time()),
        # JWT expiration time (10 minutes maximum)
        "exp": int(time.time()) + 600,
        # GitHub App's client ID
        "iss": client_id,
        "alg": "RS256",
    }

    # Create JWT
    encoded_jwt = jwt.encode(payload, signing_key, algorithm="RS256")
    return encoded_jwt


if __name__ == "__main__":
    with open("config.json", "r") as f:
        config = json.load(f)

        print(generate_jwt(config["signing_key"], config["client_id"]))
