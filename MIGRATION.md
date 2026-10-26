# Moving to <replaces>

<project> is for the long tail: small, idle most of the time, ≈ $0. When you outgrow it, the real service
is one import line away.

1. Create the real <replaces> (<how>).
2. Copy the data: <how — export command / console action>.
3. Change the import: `from <project> import <client>` → `import <client>`; the connection parameters are the
   same shape (<host, user, password, ...>).
4. Delete the stack: `terraform -chdir=terraform destroy -var-file=my.json`.

What you gain: <sessions / locks / ORM support / scale>. What you lose: the ≈ $0 idle bill.
