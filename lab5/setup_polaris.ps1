$ErrorActionPreference = "Stop"

Write-Host "1. Getting access token (ACCESS_TOKEN)..." -ForegroundColor Cyan
$tokenResponse = Invoke-RestMethod -Uri "http://localhost:8181/api/catalog/v1/oauth/tokens" `
    -Method Post `
    -Body "grant_type=client_credentials&client_id=root&client_secret=secret&scope=PRINCIPAL_ROLE:ALL" `
    -ContentType "application/x-www-form-urlencoded"

$ACCESS_TOKEN = $tokenResponse.access_token
Write-Host "Token received successfully!`n" -ForegroundColor Green

Write-Host "2. Creating Iceberg catalog (polariscatalog)..." -ForegroundColor Cyan
$catalogBody = @{
    name              = "polariscatalog"
    type              = "INTERNAL"
    properties        = @{
        "default-base-location" = "s3://warehouse"
        "s3.endpoint"           = "http://minio:9000"
        "s3.path-style-access"  = "true"
        "s3.access-key-id"      = "admin"
        "s3.secret-access-key"  = "password"
        "s3.region"             = "dummy-region"
    }
    storageConfigInfo = @{
        roleArn          = "arn:aws:iam::000000000000:role/minio-polaris-role"
        storageType      = "S3"
        allowedLocations = @("s3://warehouse/*")
    }
} | ConvertTo-Json -Depth 10

Invoke-RestMethod -Uri "http://localhost:8181/api/management/v1/catalogs" `
    -Method Post `
    -Headers @{ Authorization = "Bearer $ACCESS_TOKEN" } `
    -Body $catalogBody `
    -ContentType "application/json" | Out-Null
Write-Host "Catalog created successfully!`n" -ForegroundColor Green

Write-Host "3. Setting up permissions: creating catalog_admin role..." -ForegroundColor Cyan
$adminRoleBody = @{
    grant = @{
        type      = "catalog"
        privilege = "CATALOG_MANAGE_CONTENT"
    }
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8181/api/management/v1/catalogs/polariscatalog/catalog-roles/catalog_admin/grants" `
    -Method Put `
    -Headers @{ Authorization = "Bearer $ACCESS_TOKEN" } `
    -Body $adminRoleBody `
    -ContentType "application/json" | Out-Null
Write-Host "catalog_admin role created!`n" -ForegroundColor Green

Write-Host "4. Creating data_engineer role..." -ForegroundColor Cyan
$engineerRoleBody = @{
    principalRole = @{
        name = "data_engineer"
    }
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8181/api/management/v1/principal-roles" `
    -Method Post `
    -Headers @{ Authorization = "Bearer $ACCESS_TOKEN" } `
    -Body $engineerRoleBody `
    -ContentType "application/json" | Out-Null
Write-Host "data_engineer role created!`n" -ForegroundColor Green

Write-Host "5. Linking roles (data_engineer -> catalog_admin)..." -ForegroundColor Cyan
$connectRolesBody = @{
    catalogRole = @{
        name = "catalog_admin"
    }
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8181/api/management/v1/principal-roles/data_engineer/catalog-roles/polariscatalog" `
    -Method Put `
    -Headers @{ Authorization = "Bearer $ACCESS_TOKEN" } `
    -Body $connectRolesBody `
    -ContentType "application/json" | Out-Null
Write-Host "Roles linked successfully!`n" -ForegroundColor Green

Write-Host "6. Assigning data_engineer role to root principal..." -ForegroundColor Cyan
$rootRoleBody = @{
    principalRole = @{
        name = "data_engineer"
    }
} | ConvertTo-Json

Invoke-RestMethod -Uri "http://localhost:8181/api/management/v1/principals/root/principal-roles" `
    -Method Put `
    -Headers @{ Authorization = "Bearer $ACCESS_TOKEN" } `
    -Body $rootRoleBody `
    -ContentType "application/json" | Out-Null
Write-Host "Role assigned successfully!`n" -ForegroundColor Green

Write-Host "7. Verifying catalog creation..." -ForegroundColor Cyan
$catalogs = Invoke-RestMethod -Uri "http://localhost:8181/api/management/v1/catalogs" `
    -Method Get `
    -Headers @{ Authorization = "Bearer $ACCESS_TOKEN" }

$catalogs | ConvertTo-Json -Depth 10 | Write-Host

Write-Host "`n8. Verifying root principal roles..." -ForegroundColor Cyan
$rootRoles = Invoke-RestMethod -Uri "http://localhost:8181/api/management/v1/principals/root/principal-roles" `
    -Method Get `
    -Headers @{ Authorization = "Bearer $ACCESS_TOKEN" }

$rootRoles | ConvertTo-Json -Depth 10 | Write-Host

Write-Host "`nAll Polaris configurations completed successfully!" -ForegroundColor Yellow
