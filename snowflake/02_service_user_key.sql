-- =============================================================================
-- 02_service_user_key.sql - register the public key of the service user
--
-- Kept apart from 01_infrastructure.sql on purpose: the infrastructure is the
-- same for everyone, whereas a key pair belongs to one workstation and gets
-- rotated. Keep the placeholder in Git: paste your own key in the Snowsight
-- worksheet only.
--
-- Generate the key pair once, on the workstation that runs the tools:
--   mkdir -p ~/.ssh/snowflake && cd ~/.ssh/snowflake
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--   chmod 600 rsa_key.p8
--
-- Print the public key on a single line, without its BEGIN and END lines:
--   grep -v "BEGIN\|END" ~/.ssh/snowflake/rsa_key.pub | tr -d '\n'; echo
-- =============================================================================

USE ROLE USERADMIN;

ALTER USER AIRFLOW_SVC SET RSA_PUBLIC_KEY = '<PUBLIC_KEY_ON_ONE_LINE>';

-- Check: the RSA_PUBLIC_KEY_FP row must show the fingerprint computed locally:
--   openssl rsa -pubin -in ~/.ssh/snowflake/rsa_key.pub -outform DER \
--     | openssl dgst -sha256 -binary | openssl enc -base64
DESC USER AIRFLOW_SVC;
