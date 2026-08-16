# BusBuddyWebsite

## Update Database

1. Navigate to the `scripts` directory:
   ```bash
   cd scripts
   ```
2. Run the update script:
   ```bash
   python3 singapore.py
   ```
3. Enter the new database version when prompted.

## Deploy to Firebase Hosting

To update and deploy changes to the live Firebase hosted website:

1. **Install Firebase CLI** (if not already installed):
   ```bash
   npm install -g firebase-tools
   ```
2. **Login to Firebase** (if not already authenticated):
   ```bash
   firebase login
   ```
3. **Deploy to Firebase Hosting**:
   From the root project directory, run:
   ```bash
   firebase deploy --only hosting
   ```
   *(or run `firebase deploy` to deploy all configured Firebase services)*