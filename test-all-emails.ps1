# Email Microservice - Test All Email Types
# Sends test emails to sushmitashrestha6464@gmail.com

$baseUrl = "http://localhost:3002/api/emails/send"
$testEmail = "sushmitashrestha6464@gmail.com"
$testUser = "Sushmita Shrestha"

Write-Host "Testing Email Microservice - Sending 20 emails to $testEmail" -ForegroundColor Cyan
Write-Host ""

# Helper function to send email
function Send-TestEmail {
    param(
        [string]$Category,
        [string]$Type,
        [hashtable]$Payload,
        [string]$Description
    )
    
    Write-Host "Sending: $Description" -ForegroundColor Yellow
    
    $body = @{
        type = $Type
        payload = $Payload
    } | ConvertTo-Json
    
    try {
        $response = Invoke-RestMethod -Method Post -Uri "$baseUrl/$Category" -ContentType "application/json" -Body $body
        if ($response.success) {
            Write-Host "   Queued successfully" -ForegroundColor Green
        }
    } catch {
        Write-Host "   Failed: $($_.Exception.Message)" -ForegroundColor Red
    }
    Start-Sleep -Milliseconds 500
}

# ACCOUNT EMAILS
Write-Host "`n=== ACCOUNT EMAILS ===" -ForegroundColor Magenta
Send-TestEmail -Category "account" -Type "ACCOUNT_CREATED" -Payload @{ to = $testEmail; username = $testUser } -Description "Welcome Email"
Send-TestEmail -Category "account" -Type "PASSWORD_RESET" -Payload @{ to = $testEmail; username = $testUser; resetLink = "https://fotosfolio.com/reset/abc123" } -Description "Password Reset"
Send-TestEmail -Category "account" -Type "EMAIL_VERIFICATION" -Payload @{ to = $testEmail; userName = $testUser; otpCode = "123456" } -Description "Email Verification OTP"
Send-TestEmail -Category "account" -Type "PASSWORD_CHANGED" -Payload @{ to = $testEmail; username = $testUser } -Description "Password Changed"
Send-TestEmail -Category "account" -Type "KYC_APPROVED" -Payload @{ to = $testEmail; userName = $testUser } -Description "KYC Approved"
Send-TestEmail -Category "account" -Type "KYC_REJECTED" -Payload @{ to = $testEmail; userName = $testUser; reason = "The uploaded document image is unclear. Please upload a clearer image." } -Description "KYC Rejected"
Send-TestEmail -Category "account" -Type "TWO_FACTOR_ENABLED" -Payload @{ userEmail = $testEmail; device = "iPhone 14 Pro"; location = "Kathmandu, Nepal"; ipAddress = "192.168.1.100"; datetime = "2026-02-25T10:30:00Z" } -Description "Two-Factor Authentication Enabled"

# SUBSCRIPTION EMAILS
Write-Host "`n=== SUBSCRIPTION EMAILS ===" -ForegroundColor Magenta
Send-TestEmail -Category "subscription" -Type "SUBSCRIPTION_STARTED" -Payload @{ to = $testEmail; userName = $testUser; planName = "Pro Plan"; startDate = "2026-02-25"; endDate = "2026-03-25"; status = "active" } -Description "Subscription Started"
Send-TestEmail -Category "subscription" -Type "SUBSCRIPTION_RENEWED" -Payload @{ to = $testEmail; userName = $testUser; planName = "Pro Plan"; startDate = "2026-03-25"; endDate = "2026-04-25"; status = "active" } -Description "Subscription Renewed"
Send-TestEmail -Category "subscription" -Type "SUBSCRIPTION_CANCELLED" -Payload @{ to = $testEmail; userName = $testUser; graceDaysRemaining = 7 } -Description "Subscription Cancelled"
Send-TestEmail -Category "subscription" -Type "SUBSCRIPTION_EXPIRING" -Payload @{ to = $testEmail; userName = $testUser; daysRemaining = 3 } -Description "Subscription Expiring Soon"
Send-TestEmail -Category "subscription" -Type "PLAN_UPGRADED" -Payload @{ to = $testEmail; userName = $testUser; planName = "Premium Plan" } -Description "Plan Upgraded"

# SECURITY EMAILS
Write-Host "`n=== SECURITY EMAILS ===" -ForegroundColor Magenta
Send-TestEmail -Category "security" -Type "LOGIN_ALERT" -Payload @{ userEmail = $testEmail; ipAddress = "192.168.1.100"; device = "Chrome on Windows"; location = "Kathmandu, Nepal"; datetime = "2026-02-25T10:30:00Z" } -Description "New Login Alert"
Send-TestEmail -Category "security" -Type "SUSPICIOUS_ACTIVITY" -Payload @{ to = $testEmail; userName = $testUser; loginTime = "2026-02-25T10:45:00Z"; ipAddress = "203.0.113.42" } -Description "Suspicious Activity Detected"

# PROJECT EMAILS
Write-Host "`n=== PROJECT EMAILS ===" -ForegroundColor Magenta
Send-TestEmail -Category "project" -Type "PROJECT_SHARED" -Payload @{ receiverEmail = $testEmail; receiverName = $testUser; senderName = "Jane Smith"; projectName = "Wedding Photography"; projectLink = "https://fotosfolio.com/projects/456"; totalFiles = 150; totalSize = "2.5 GB" } -Description "Project Shared"
Send-TestEmail -Category "project" -Type "GALLERY_SHARED" -Payload @{ receiverEmail = $testEmail; inviterName = "John Doe"; projectName = "Nepal Travel Photos"; projectLink = "https://fotosfolio.com/projects/123" } -Description "Gallery Shared"
Send-TestEmail -Category "project" -Type "FOLDER_SHARED" -Payload @{ projectName = "Event Coverage"; requesterEmail = "mike@example.com"; ownerEmail = $testEmail; photographerName = "Mike Johnson"; projectId = "789" } -Description "Folder Access Request"

# PAYMENT EMAILS
Write-Host "`n=== PAYMENT EMAILS ===" -ForegroundColor Magenta
Send-TestEmail -Category "payment" -Type "PAYMENT_SUCCESS" -Payload @{ to = $testEmail; userName = $testUser; amount = 29.99; currency = "USD"; transactionId = "TXN123456"; planName = "Pro Plan" } -Description "Payment Successful"
Send-TestEmail -Category "payment" -Type "PAYMENT_FAILED" -Payload @{ to = $testEmail; userName = $testUser; amount = 29.99; currency = "USD"; reason = "Insufficient funds"; retryUrl = "https://fotosfolio.com/billing/retry" } -Description "Payment Failed"
Send-TestEmail -Category "payment" -Type "REFUND_PROCESSED" -Payload @{ to = $testEmail; userName = $testUser; amount = 29.99; currency = "USD"; transactionId = "REF789012"; reason = "Service cancellation" } -Description "Refund Processed"

# STORAGE EMAILS
Write-Host "`n=== STORAGE EMAILS ===" -ForegroundColor Magenta
Send-TestEmail -Category "storage" -Type "STORAGE_WARNING" -Payload @{ to = $testEmail; userName = $testUser; storageUsed = 8.5; storageLimit = 10; percentUsed = 85 } -Description "Storage Warning (85% used)"
Send-TestEmail -Category "storage" -Type "STORAGE_FULL" -Payload @{ to = $testEmail; userName = $testUser; storageLimit = 10 } -Description "Storage Full (100% used)"

# SUMMARY
Write-Host "`n====================" -ForegroundColor Cyan
Write-Host "Test Complete!" -ForegroundColor Green
Write-Host "Check your email: $testEmail" -ForegroundColor Yellow
Write-Host "Emails may take a few seconds" -ForegroundColor Gray
