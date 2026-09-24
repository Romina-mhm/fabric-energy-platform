/* =====================================================================
   01_master_fabric_login.sql
   RUN AGAINST:  the  master  database of your Azure SQL logical server
   (In Azure SQL you cannot "USE master" from a user DB — open a separate
    connection/query window pointed at master.)

   Purpose: a dedicated, least-privilege login that Fabric Mirroring will
   use to read the database. Never let Fabric connect as the server admin.

   Production note: in a real company you'd prefer Microsoft Entra auth
   (workspace identity or a service principal) — no password to rotate.
   SQL auth is used here because it is the simplest thing that works on a
   personal tenant; we'll revisit this in the Security phase.
   ===================================================================== */

-- Replace the password with a strong one and store it in a password
-- manager. You'll type it once into the Fabric connection dialog.
IF NOT EXISTS (SELECT 1 FROM sys.sql_logins WHERE name = N'fabric_mirror_login')
    CREATE LOGIN [fabric_mirror_login] WITH PASSWORD = N'<REPLACE-with-a-strong-password>';
GO

-- Sanity check: the server's system-assigned managed identity must exist
-- (you switch it on in the portal: SQL server > Security > Identity).
-- Mirroring uses it to push changes into OneLake.
-- This query is valid from master or the user DB; expect one row.
SELECT * FROM sys.dm_server_managed_identities;
GO
