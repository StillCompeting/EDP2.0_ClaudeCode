#!/bin/bash
echo "Extracting public key from /Users/paulhopkins/.snowsql/rsa_key.p8"
echo "Enter passphrase: Bears@Rainbows2025!"
echo ""
openssl rsa -in /Users/paulhopkins/.snowsql/rsa_key.p8 -pubout -outform DER -passin pass:"Bears@Rainbows2025!" 2>/dev/null | openssl base64 -A
echo ""
echo ""
echo "Copy the output above and run this SQL in Snowflake:"
echo "ALTER USER StillCompeting SET RSA_PUBLIC_KEY='<paste_output_here>';"
