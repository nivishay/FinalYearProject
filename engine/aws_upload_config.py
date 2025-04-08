aws_config = {
  "source": {
    "DBInstanceIdentifier": "mysql-source-db",
    "DBName": "sakila",
    "Engine": "mysql",
    "MasterUsername": "admin",
    "MasterUserPassword": "StrongPassword123",
    "DBInstanceClass": "db.t3.micro",
    "AllocatedStorage": 20
  },
  "destination": {
    "DBInstanceIdentifier": "postgres-dest-db",
    "DBName": "sakila_migrated",
    "Engine": "postgres",
    "MasterUsername": "pgadmin",
    "MasterUserPassword": "StrongPassword456",
    "DBInstanceClass": "db.t3.micro",
    "AllocatedStorage": 20
  }
}
