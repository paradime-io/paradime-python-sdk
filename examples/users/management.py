# First party modules
from paradime import Paradime
from paradime.apis.users.types import UserAccountType

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")


# Get all active users
active_users = paradime.users.list_active()

# Get a user by email
user = paradime.users.get_by_email(email="bhuvan@paradime.io")

# Invite a user as an admin
paradime.users.invite(email="bhuvan@paradime.io", account_type=UserAccountType.ADMIN)

# Get all invited users
invited_users = paradime.users.list_invited()

# Update a user's account type
paradime.users.update_account_type(user_uid=user.uid, account_type=UserAccountType.DEVELOPER)

# Disable a user
paradime.users.disable(user_uid=user.uid)
