// src/email-services/account-emails.service.ts
import { Injectable } from '@nestjs/common';
import { BaseEmailService, EmailData } from './base-email.service';

@Injectable()
export class AccountEmailsService extends BaseEmailService {
  private getCurrentYear(): number {
    return new Date().getFullYear();
  }

  async buildAccountCreatedEmail(
    to: string,
    username: string,
  ): Promise<EmailData> {
    console.log(`Sending account created email to ${to}`);
    const subject = 'Welcome to Fotosfolio – Your Account is Ready!';

    const text = `Welcome, ${username}!

  Your account has been successfully created.
  Now start showcasing your photography and meet other creatives.

  Access your Fotosfolio account:
  https://fotosfolio.com/login

  Need help getting started?
  Just reply to this mail or visit our Help Center.

  Warm wishes,
  The Fotosfolio Team
  `;

    const html = `
    <div style="background-color:#f9f9f9; padding:40px 0; font-family:Arial, sans-serif; color:#333;">
      <div style="max-width:600px; margin:0 auto; background:#ffffff; border-radius:10px; padding:40px 50px; box-shadow:0 0 10px rgba(0,0,0,0.05);">

        <!-- Header: Logo + Social Icons -->
        <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:30px;">
          <tr>
            <td style="text-align:left;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; object-fit:contain;" />
            </td>
            <td style="text-align:right;">
              <a href="https://www.instagram.com/fotosfolio.np/" target="_blank" style="margin-right:10px;">
                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" />
              </a>
              <a href="https://linkedin.com/company/fotosfolio" target="_blank" style="margin-right:10px;">
                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" />
              </a>
              <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank" style="margin-right:10px;">
                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" />
              </a>
              <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" />
              </a>
            </td>
          </tr>
        </table>

        <!-- Body -->
        <div style="text-align:center;">
          <h2 style="color:#7a1217; font-size:18px; margin-bottom:20px;">Welcome, ${username}</h2>

          <p style="font-size:13px; color:#666; margin:0 0 25px;">
            Thanks for signing up for Fotosfolio — your private and secure cloud storage solution.
          </p>

          <p style="font-size:14px; color:#333; margin-bottom:5px; font-weight:600;">
            Your account has been successfully created!
          </p>
          <p style="font-size:13px; color:#333; margin-bottom:30px;">
            Now start showcasing your photography and meet other creatives.
          </p>

          <p style="font-size:12px; color:#7a1217; margin-bottom:15px;">
            Ready to get started? Click below to access your dashboard.
          </p>

          <p style="margin:25px 0;">
            <a href="https://fotosfolio.com/login"
               style="background-color:#7a1217; color:#fff; text-decoration:none;
                      padding:10px 25px; border-radius:20px; font-size:13px; display:inline-block;">
              Access My Fotosfolio Account
            </a>
          </p>

          <hr style="border:none; border-top:1px solid #eee; margin:40px 0;">

          <p style="font-size:12px; color:#333; font-weight:600; margin-bottom:10px;">
            Need Help Getting Started?
          </p>
          <p style="font-size:12px; color:#666; margin:0 0 25px;">
            If you have any questions or feedback, we’re here to help! Just
            <a href="mailto:support@fotosfolio.com" style="color:#7a1217; text-decoration:none;">Send mail</a>
            or visit our <a href="https://fotosfolio.com/help" style="color:#7a1217; text-decoration:none;">Help Center</a>.
          </p>

          <p style="font-size:12px; color:#333; margin:30px 0 5px;">Warm wishes,</p>
          <p style="font-size:12px; font-weight:600; color:#333; margin:0;">The Fotosfolio Team</p>
        </div>
      </div>

      <div style="max-width:600px; margin:0 auto; text-align:center; padding:20px 0; font-size:11px; color:#999;">
        <p style="margin:5px 0;">© ${this.getCurrentYear()} Fotosfolio. All rights reserved.</p>
        <p style="margin:5px 0;">
          <a href="https://fotosfolio.com/privacy" style="color:#999; text-decoration:none;">Privacy Policy</a> |
          <a href="https://fotosfolio.com/terms-and-conditions" style="color:#999; text-decoration:none;">Terms of Service</a> |
          <a href="https://fotosfolio.com/contact-us" style="color:#999; text-decoration:none;">Contact Support</a> |
          <a href="https://fotosfolio.com/unsubscribe" style="color:#999; text-decoration:none;">Unsubscribe</a>
        </p>
      </div>
    </div>`;
    return { to, subject, text, html };
  }

  async buildPasswordResetEmail(
    to: string,
    username: string,
    resetLink: string,
  ): Promise<EmailData> {
    console.log('sdfsdfsdf');
    const subject = 'Reset Your Fotosfolio Password';

    const text = `Hi ${username},

  You requested to reset your Fotosfolio password.

  Click the Button below to securely create a new password and regain access to your account:
  ${resetLink}

  This link will expire in 10 minutes. If you didn’t request this change, please ignore this email or contact Fotosfolio Support.

  – The Fotosfolio Team`;

    const html = `
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Password Reset</title>
    </head>
    <body style="margin:0; padding:0; background-color:#f3f3f3; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;">

    <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#f3f3f3; padding:28px 0;">
      <tr>
        <td align="center">
          <table width="600" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#ffffff; border-radius:10px; box-shadow:0 2px 6px rgba(0,0,0,0.05); overflow:hidden;">

            <!-- Header -->
            <tr>
              <td style="padding:20px 30px; border-bottom:1px solid #f0f0f0;">
                <table width="100%" cellpadding="0" cellspacing="0">
                  <tr>
                    <td align="left">
                      <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; object-fit:contain;" />
                    </td>
                    <td align="right" style="vertical-align:middle;">
                      <table cellpadding="0" cellspacing="0" style="display:inline-block;">
                        <tr>
                          <td><a href="https://www.instagram.com/fotosfolio.np/"><img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" style="margin-left:6px;" /></a></td>
                          <td><a href="https://linkedin.com/company/fotosfolio"><img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" style="margin-left:6px;" /></a></td>
                          <td><a href="https://www.facebook.com/profile.php?id=61575539292098"><img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" style="margin-left:6px;" /></a></td>
                          <td><a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M"><img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" style="margin-left:6px;" /></a></td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <!-- Body -->
            <tr>
              <td style="padding:40px 36px;">
                <h2 style="color:#7b0f0f; font-size:22px; font-weight:600; margin:0 0 10px;">Reset Your Password</h2>
                <p style="color:#555; font-size:14px; margin:0 0 25px;">Hi <strong>${username}</strong>, let’s get you securely back into your account.</p>

                <div style="background-color:#fef3f3; border-left:4px solid #b32020; border-radius:8px; margin-bottom:28px; padding:16px 20px; color:#b32020; font-size:14px; line-height:1.6;">
                  <p style="margin:0;">You requested to reset your password. Click button below to set a new one securely. This link is valid for <strong>10 minutes</strong>.</p>
                </div>

                <div style="text-align:center; margin:30px 0 35px 0;">
                  <a href="${resetLink}" style="display:inline-block; background-color:#7b0f0f; color:#ffffff; text-decoration:none; padding:12px 32px; border-radius:25px; font-weight:500; font-size:15px; box-shadow:0 2px 8px rgba(179,32,32,0.15);">Reset Password</a>
                </div>

                <p style="font-size:13px; color:#555; text-align:center; margin:0 0 10px;">If the button above doesn’t work, you can also copy and paste this link:</p>
                <div style="background:#f7f7f7; border:1px solid #e0e0e0; border-radius:8px; padding:10px 15px; word-break:break-all; font-size:13px; color:#7b0f0f; text-align:left;">
                  <a href="${resetLink}" style="color:#7b0f0f; text-decoration:none;">${resetLink}</a>
                </div>

                <p style="font-size:13px; color:#555; text-align:center; line-height:1.5; margin-top:25px;">
                If you did not request a password reset, you can safely ignore this email.
                 If you believe this was not you or need assistance, please
                 <a href="https://fotosfolio.com/contact-us" style="color:#b32020; text-decoration:none;">contact our support team</a>.
                </p>
              </td>
            </tr>

            <!-- Footer -->
            <tr>
              <td style="padding:20px 10px; border-top:1px solid #eee; background-color:#fafafa; text-align:center;">
                <p style="font-size:12px; color:#999; margin:8px 0;">© ${this.getCurrentYear()} Fotosfolio. All rights reserved.</p>
                <p style="font-size:12px; color:#999; margin:0;">
                  <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
                  <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
                  <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
                </p>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>
    </body>
    </html>
    `;
    return { to, subject, text, html };
  }

  async buildPasswordResetSuccessEmail(
    to: string,
    username: string,
  ): Promise<EmailData> {
    const subject = ' Password Reset Successful';
    const text = `
  Password Reset Successful

  Hi ${username},

  Your Fotosfolio account password has been reset successfully.
  You can now log in using your new password.
  Go to Login
  ⚠️ If you didn’t request a password change, please contact our support team immediately to secure your account.`;

    const html = `
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Password Reset Successful</title>
    </head>
    <body style="margin:0; padding:0; background-color:#f3f3f3; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">

    <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#f3f3f3; padding:28px 0;">
      <tr>
        <td align="center">
          <table width="600" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#ffffff; border-radius:10px; box-shadow:0 2px 6px rgba(0,0,0,0.05); overflow:hidden;">

            <!-- Header (same layout as sendNewIpLoginAlertMail) -->
            <tr>
              <td style="padding:20px 30px; border-bottom:1px solid #f0f0f0;">
                <table width="100%" cellpadding="0" cellspacing="0">
                  <tr>
                    <td align="left" valign="middle" style="vertical-align:middle;">
                      <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; object-fit:contain; border:0; outline:none;" />
                    </td>
                    <td align="right" valign="middle" style="vertical-align:middle;">
                      <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                        <tr>
                          <td style="padding:0 6px 0 0;">
                            <a href="https://www.instagram.com/fotosfolio.np/" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="22" height="22" alt="Instagram" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 6px;">
                            <a href="https://linkedin.com/company/fotosfolio" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="22" height="22" alt="LinkedIn" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 6px;">
                            <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="22" height="22" alt="Facebook" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 0 0 6px;">
                            <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="22" height="22" alt="WhatsApp" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <!-- Body (exact original content preserved, styled to match approved layout) -->
            <tr>
              <td style="padding:40px 36px 28px 36px; text-align:center;">

                <!-- small icon row to match approved visual -->
                <div style="margin-bottom:14px;">
                  <div style="width:64px;height:64px;border-radius:50%;background-color:#e8f8ef;display:inline-flex;align-items:center;justify-content:center;font-size:28px;line-height:1;color:#007f3d;">
                    ✅
                  </div>
                </div>

                <!-- Title -->
                <h2 style="font-size:22px;font-weight:600;color:#7b0f0f;margin:6px 0 12px 0;">Password Reset Successful</h2>

                <!-- Greeting -->
                <p style="font-size:15px;color:#333;margin:6px 0 8px 0;">Hi <strong>${username}</strong>,</p>

                <!-- Main message lines (kept exactly as original content) -->
                <p style="font-size:14px;color:#555;line-height:1.6;margin:0 0 18px 0;">
                  Your Fotosfolio account password has been reset successfully.<br />
                  You can now log in using your new password.
                </p>

                <!-- CTA button (label: Go to Login) -->
                <div style="margin:18px 0 10px 0;">
                  <a href="https://fotosfolio.com/login"
                     style="display:inline-block;background-color:#7b0f0f;color:#ffffff;text-decoration:none;padding:12px 28px;border-radius:25px;font-weight:500;font-size:15px;box-shadow:0 2px 8px rgba(179,32,32,0.15);">
                    Go to Login
                  </a>
                </div>

                <!-- Warning / contact sentence (exact original) -->
                <div style="margin-top:20px;max-width:520px;margin-left:auto;margin-right:auto;background-color:#fdf6f6;border:1px solid #f0dcdc;border-radius:8px;padding:12px 14px;text-align:center;color:#7a1217;font-size:13px;line-height:1.5;">
                  ⚠️ If you didn’t request this password change, please
                  <a href="https://fotosfolio.com/contact-us" style="color:#7b0f0f;text-decoration:none;">contact our support team</a>
                  immediately to secure your account.
                </div>


              </td>
            </tr>

            <!-- Footer (same as sendNewIpLoginAlertMail; current year and three links only) -->
            <tr>
              <td style="padding:20px 10px; border-top:1px solid #eee; background-color:#fafafa; text-align:center;">
                <p style="font-size:12px; color:#999; margin:8px 0;">© ${this.getCurrentYear()} Fotosfolio. All rights reserved.</p>
                <p style="font-size:12px; color:#999; margin:0;">
                  <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
                  <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
                  <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
                </p>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>
    </body>
    </html>
    `;
    console.log('Password reset success email sent successfully');
    return { to, subject, text, html };
  }

  async buildOtpEmail(
    to: string,
    userName: string,
    otpCode: string,
  ): Promise<EmailData> {
    const subject = 'Your OTP Code for Secure Access';
    const text = `Dear ${userName},
  We have received a request to verify your email address for your Fotosfolio account.
  Please use the verification code below to complete the authentication process.

  Verification Code: ${otpCode}

  This code expires in 2 minutes.
  Please do not share this code with anyone for security purposes.

  If you did not request this verification code or require assistance, please contact our Support Team immediately.

  — The Fotosfolio Team
  `;

    const html = `
    <div style="font-family:'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background-color:#fafafa;padding:40px 0;">
      <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 14px rgba(0,0,0,0.05);box-sizing:border-box;">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;box-sizing:border-box;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:12px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; border:none; outline:none; text-decoration:none;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:12px;">
              <table cellpadding="0" cellspacing="0" role="presentation">
                <tr>
                  <td style="padding:0 6px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block;border:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block;border:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block;border:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block;border:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Title -->
        <div style="text-align:center;padding:20px 25px 8px 25px;">
          <div style="font-size:34px;color:#7a1217;margin-bottom:8px;">🔐</div>
          <h2 style="color:#7a1217;font-size:22px;font-weight:600;margin:0;">Your Verification Code</h2>
          <p style="margin-top:6px;color:#555;font-size:14px;">Use the OTP below to verify your email address.</p>
        </div>

        <!-- Body -->
        <div style="padding:0 40px 30px 40px;text-align:left;box-sizing:border-box;">
          <p style="font-size:15px;color:#333;margin:0 0 10px 0;">Dear ${userName},</p>
          <p style="font-size:14px;color:#555;margin:0 0 18px 0;">
            We have received a request to verify your email address for your Fotosfolio account.
            Please use the verification code below to complete the authentication process.
          </p>

          <!-- OTP Box -->
          <div style="border:1px solid #e8e8e8;border-radius:10px;background:#faf9f9;width:100%;margin:0 auto 25px;text-align:center;padding:25px 0;">
            <div style="font-size:13px;color:#777;margin-bottom:8px;">Verification Code</div>
            <div style="font-size:32px;font-weight:700;color:#7a1217;letter-spacing:3px;">${otpCode}</div>
            <div style="font-size:13px;color:#999;margin-top:10px;">This code expires in 10 minutes</div>
            <div style="font-size:12px;color:#b32020;margin-top:6px;">Do not share this code with anyone</div>
          </div>

          <!-- Help Box -->
          <div style="background-color:#faf9f9;border:1px solid #f2f2f2;border-radius:10px;text-align:center;padding:20px 25px;font-size:13px;color:#555;margin-bottom:25px;box-sizing:border-box;">
            <strong>Need Help?</strong><br />
            If you didn’t request this verification, please contact our
            <a href="https://fotosfolio.com/contact-us" style="color:#7a1217;text-decoration:none;">Support Team</a> immediately.
          </div>

          <!-- Signature -->
          <p style="text-align:center;color:#333;font-size:14px;font-weight:600;margin-bottom:0;">
            — The Fotosfolio Team
          </p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px 10px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${this.getCurrentYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717;text-decoration:none;margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717;text-decoration:none;margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717;text-decoration:none;margin:0 6px;">Contact Support</a>
          </p>
        </div>
      </div>
    </div>
    `;
    return { to, subject, text, html };
  }

  async buildOtpVerifiedEmail(
    to: string,
    userName: string,
  ): Promise<EmailData> {
    console.log(to, userName);
    const subject = 'Email Verification Completed Successfully';
    const text = `Dear ${userName},

  Your email verification has been completed successfully. Your account security has been confirmed, and you now have full access to your account.

  All security protocols have been satisfied, and your session is now authenticated.

  Thank you for helping us maintain the security of your account.

  Best regards,
  The Fotosfolio Team
  Privacy Policy: https://fotosfolio.com/privacy
  Terms of Service: https://fotosfolio.com/terms-and-conditions
  Contact Support: https://fotosfolio.com/contact-us
  `;

    const html = `
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <meta http-equiv="x-ua-compatible" content="ie=edge">
      <title>Email Verification Complete</title>
    </head>
    <body style="margin:0; padding:0; background-color:#f5f5f5; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;">

      <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#f5f5f5; padding:30px 0;">
        <tr>
          <td align="center">

            <table width="600" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#ffffff; border-radius:10px; box-shadow:0 2px 8px rgba(0,0,0,0.05); overflow:hidden;">

              <!-- Header -->
              <tr>
                <td style="padding:20px 25px; border-bottom:1px solid #eee;">
                  <table width="100%" cellpadding="0" cellspacing="0" role="presentation">
                    <tr>
                      <!-- Logo -->
                      <td align="left" style="vertical-align:middle;">
                        <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; border:0; outline:none; text-decoration:none;">
                      </td>

                      <!-- Icons -->
                      <td align="right" style="vertical-align:middle;">
                        <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                          <tr>
                            <td style="padding:0 4px;">
                              <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block;">
                              </a>
                            </td>
                            <td style="padding:0 4px;">
                              <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block;">
                              </a>
                            </td>
                            <td style="padding:0 4px;">
                              <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block;">
                              </a>
                            </td>
                            <td style="padding:0 4px;">
                              <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block;">
                              </a>
                            </td>
                          </tr>
                        </table>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>

              <!-- Main Content -->
              <tr>
                <td style="padding:40px 36px;">
                  <p style="font-size:16px; color:#333333; margin-bottom:10px;">Dear ${userName},</p>

                  <p style="font-size:14px; color:#555555; line-height:1.6; margin-bottom:20px;">
                    Your email has been successfully verified. You now have full access to your Fotosfolio account.
                    Your security verification has been completed, and your session is authenticated.
                  </p>

                  <table width="100%" role="presentation" style="background-color:#e8f5e9; border:1px solid #c3e6cb; border-radius:8px; text-align:center; padding:24px; margin-bottom:24px;">
                    <tr>
                      <td style="font-size:12px; color:#155724; padding-bottom:6px;">Status</td>
                    </tr>
                    <tr>
                      <td style="font-size:32px; font-weight:700; color:#28a745; letter-spacing:1px;">✓ VERIFIED</td>
                    </tr>
                    <tr>
                      <td style="font-size:13px; color:#155724;">Account security confirmed & session authenticated</td>
                    </tr>
                  </table>

                  <p style="font-size:13px; color:#555555; line-height:1.6; margin-bottom:20px;">
                    All security protocols have been satisfied. Thank you for helping us maintain the security of your account.
                  </p>

                  <p style="font-size:14px; color:#333333; margin-top:30px;">
                    Best regards,<br>
                    <strong>The Fotosfolio Team</strong>
                  </p>
                </td>
              </tr>

              <!-- Footer -->
              <tr>
                <td style="padding:24px; border-top:1px solid #eee; text-align:center;">
                  <p style="font-size:12px; color:#999; margin:0;">
                    © ${this.getCurrentYear()} Fotosfolio. All rights reserved.<br>
                    <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
                    <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
                    <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
                  </p>
                </td>
              </tr>

            </table>
          </td>
        </tr>
      </table>

    </body>
    </html>
    `;
    return { to, subject, text, html };
  }

  async buildKycApprovedEmail(
    to: string,
    userName: string,
  ): Promise<EmailData> {
    const subject = 'Your KYC Verification Has Been Approved';
    const text = `Hi ${userName},

Your KYC verification has been approved successfully.

Your account is now verified, and you can continue using Fotosfolio without interruption.

If you have any questions, please contact our support team.

Best regards,
The Fotosfolio Team`;

    const html = `
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>KYC Approved</title>
    </head>
    <body style="margin:0; padding:0; background-color:#f3f3f3; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">

    <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#f3f3f3; padding:28px 0;">
      <tr>
        <td align="center">
          <table width="600" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#ffffff; border-radius:10px; box-shadow:0 2px 6px rgba(0,0,0,0.05); overflow:hidden;">

            <tr>
              <td style="padding:20px 30px; border-bottom:1px solid #f0f0f0;">
                <table width="100%" cellpadding="0" cellspacing="0">
                  <tr>
                    <td align="left" valign="middle" style="vertical-align:middle;">
                      <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; object-fit:contain; border:0; outline:none;" />
                    </td>
                    <td align="right" valign="middle" style="vertical-align:middle;">
                      <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                        <tr>
                          <td style="padding:0 6px 0 0;">
                            <a href="https://www.instagram.com/fotosfolio.np/" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="22" height="22" alt="Instagram" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 6px;">
                            <a href="https://linkedin.com/company/fotosfolio" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="22" height="22" alt="LinkedIn" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 6px;">
                            <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="22" height="22" alt="Facebook" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 0 0 6px;">
                            <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="22" height="22" alt="WhatsApp" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <tr>
              <td style="padding:40px 36px 28px 36px; text-align:center;">
                <h2 style="font-size:22px;font-weight:600;color:#7b0f0f;margin:6px 0 12px 0;">KYC Verification Approved</h2>

                <p style="font-size:15px;color:#333;margin:6px 0 8px 0;">Hi <strong>${userName}</strong>,</p>

                <p style="font-size:14px;color:#555;line-height:1.6;margin:0 0 18px 0;">
                  Your KYC verification has been approved successfully.
                </p>

                <div style="margin:20px auto;max-width:480px;background-color:#f6fbf7;border:1px solid #d6eadb;border-radius:8px;padding:16px 20px;text-align:left;">
                  <p style="font-size:13px;color:#666;margin:4px 0;">Your verification is complete</p>
                  <p style="font-size:13px;color:#666;margin:4px 0;">You can now securely access and use all Fotosfolio features without restrictions.</p>
                </div>

                <div style="margin:18px 0 10px 0;">
                  <a href="https://fotosfolio.com/login"
                     style="display:inline-block;background-color:#7b0f0f;color:#ffffff;text-decoration:none;padding:12px 28px;border-radius:25px;font-weight:500;font-size:15px;box-shadow:0 2px 8px rgba(179,32,32,0.15);">
                    Go to Your Account
                  </a>
                </div>

                <p style="font-size:13px;color:#555;line-height:1.6;max-width:520px;margin:20px auto 0 auto;">
                  If you have any questions or need help, please contact our support team.
                </p>
              </td>
            </tr>

            <tr>
              <td style="padding:20px 10px; border-top:1px solid #eee; background-color:#fafafa; text-align:center;">
                <p style="font-size:12px; color:#999; margin:8px 0;">&copy; ${this.getCurrentYear()} Fotosfolio. All rights reserved.</p>
                <p style="font-size:12px; color:#999; margin:0;">
                  <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> &bull;
                  <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> &bull;
                  <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
                </p>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>

    </body>
    </html>
    `;

    return { to, subject, text, html };
  }

  async buildKycRejectedEmail(
    to: string,
    userName: string,
    reason?: string,
  ): Promise<EmailData> {
    const rejectionReason =
      reason?.trim() || 'The submitted documents could not be verified.';
    const subject = 'Your KYC Verification Was Rejected';
    const text = `Hi ${userName},

We reviewed your KYC submission, but we were unable to approve it at this time.

Reason:
${rejectionReason}

Please update your documents and submit your KYC again.

If you need help, please contact our support team.

Best regards,
The Fotosfolio Team`;

    const html = `
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>KYC Rejected</title>
    </head>
    <body style="margin:0; padding:0; background-color:#f3f3f3; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">

    <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#f3f3f3; padding:28px 0;">
      <tr>
        <td align="center">
          <table width="600" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#ffffff; border-radius:10px; box-shadow:0 2px 6px rgba(0,0,0,0.05); overflow:hidden;">

            <tr>
              <td style="padding:20px 30px; border-bottom:1px solid #f0f0f0;">
                <table width="100%" cellpadding="0" cellspacing="0">
                  <tr>
                    <td align="left" valign="middle" style="vertical-align:middle;">
                      <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; object-fit:contain; border:0; outline:none;" />
                    </td>
                    <td align="right" valign="middle" style="vertical-align:middle;">
                      <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                        <tr>
                          <td style="padding:0 6px 0 0;">
                            <a href="https://www.instagram.com/fotosfolio.np/" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="22" height="22" alt="Instagram" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 6px;">
                            <a href="https://linkedin.com/company/fotosfolio" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="22" height="22" alt="LinkedIn" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 6px;">
                            <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="22" height="22" alt="Facebook" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 0 0 6px;">
                            <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="22" height="22" alt="WhatsApp" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <tr>
              <td style="padding:40px 36px 28px 36px; text-align:center;">
                <h2 style="font-size:22px;font-weight:600;color:#7b0f0f;margin:6px 0 12px 0;">KYC Verification Rejected</h2>

                <p style="font-size:15px;color:#333;margin:6px 0 8px 0;">Hi <strong>${userName}</strong>,</p>

                <p style="font-size:14px;color:#555;line-height:1.6;margin:0 0 18px 0;">
                  We reviewed your KYC submission, but we were unable to approve it at this time.
                </p>

                <div style="margin:20px auto;max-width:480px;background-color:#fff8f1;border:1px solid #f2dec2;border-radius:8px;padding:16px 20px;text-align:left;">
                  <p style="font-size:13px;font-weight:600;color:#333;margin:0 0 10px 0;">Reason for rejection:</p>
                  <p style="font-size:13px;color:#666;margin:0;line-height:1.6;">${rejectionReason}</p>
                </div>

                <p style="font-size:13px;color:#555;line-height:1.6;max-width:520px;margin:0 auto 18px auto;">
                  Please review your information, update the required documents, and submit your KYC again.
                </p>

                <div style="margin:18px 0 10px 0;">
                  <a href="https://fotosfolio.com/settings"
                     style="display:inline-block;background-color:#7b0f0f;color:#ffffff;text-decoration:none;padding:12px 28px;border-radius:25px;font-weight:500;font-size:15px;box-shadow:0 2px 8px rgba(179,32,32,0.15);">
                    Update Your Documents
                  </a>
                </div>

                <div style="margin-top:20px;max-width:520px;margin-left:auto;margin-right:auto;background-color:#fdf6f6;border:1px solid #f0dcdc;border-radius:8px;padding:12px 14px;text-align:center;color:#7a1217;font-size:13px;line-height:1.5;">
                  Need help with resubmission? Please
                  <a href="https://fotosfolio.com/contact-us" style="color:#7b0f0f;text-decoration:none;">contact our support team</a>.
                </div>
              </td>
            </tr>

            <tr>
              <td style="padding:20px 10px; border-top:1px solid #eee; background-color:#fafafa; text-align:center;">
                <p style="font-size:12px; color:#999; margin:8px 0;">&copy; ${this.getCurrentYear()} Fotosfolio. All rights reserved.</p>
                <p style="font-size:12px; color:#999; margin:0;">
                  <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> &bull;
                  <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> &bull;
                  <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
                </p>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>

    </body>
    </html>
    `;

    return { to, subject, text, html };
  }

  async buildGoogleAccountDisconnectedEmail(
    to: string,
    username: string,
    device: string,
    location: string,
    ipAddress: string,
    datetime: Date,
  ): Promise<EmailData> {
    const subject = 'Google Account Disconnected';
    const formattedDate = datetime
      ? new Date(datetime).toLocaleString('en-US', {
          year: 'numeric',
          month: 'long',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          timeZoneName: 'short',
        })
      : new Date().toLocaleString('en-US', {
          year: 'numeric',
          month: 'long',
          day: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          timeZoneName: 'short',
        });

    const text = `
  Google Account Disconnected

  Hi ${username},

  Your Google account has been successfully disconnected from your Fotosfolio account.

  Disconnection Details:
  ${datetime ? `Time: ${formattedDate}` : ''}
  ${ipAddress ? `IP Address: ${ipAddress}` : ''}
  ${location ? `Location: ${location}` : ''}
  ${device ? `Device: ${device}` : ''}

  You can now log in using your email and password.
  If you previously only used Google to sign in, please make sure to set up a password for your account.

  Go to Account Settings

  ⚠️ If you didn't disconnect your Google account, please contact our support team immediately to secure your account.`;

    const html = `
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>Google Account Disconnected</title>
    </head>
    <body style="margin:0; padding:0; background-color:#f3f3f3; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;">

    <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#f3f3f3; padding:28px 0;">
      <tr>
        <td align="center">
          <table width="600" cellpadding="0" cellspacing="0" role="presentation" style="background-color:#ffffff; border-radius:10px; box-shadow:0 2px 6px rgba(0,0,0,0.05); overflow:hidden;">

            <!-- Header -->
            <tr>
              <td style="padding:20px 30px; border-bottom:1px solid #f0f0f0;">
                <table width="100%" cellpadding="0" cellspacing="0">
                  <tr>
                    <td align="left" valign="middle" style="vertical-align:middle;">
                      <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; object-fit:contain; border:0; outline:none;" />
                    </td>
                    <td align="right" valign="middle" style="vertical-align:middle;">
                      <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                        <tr>
                          <td style="padding:0 6px 0 0;">
                            <a href="https://www.instagram.com/fotosfolio.np/" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="22" height="22" alt="Instagram" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 6px;">
                            <a href="https://linkedin.com/company/fotosfolio" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="22" height="22" alt="LinkedIn" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 6px;">
                            <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="22" height="22" alt="Facebook" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                          <td style="padding:0 0 0 6px;">
                            <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank" style="text-decoration:none;">
                              <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="22" height="22" alt="WhatsApp" style="display:block;border:0;outline:none;" />
                            </a>
                          </td>
                        </tr>
                      </table>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>

            <!-- Body -->
            <tr>
              <td style="padding:40px 36px 28px 36px; text-align:center;">

                <!-- Icon -->
                <div style="margin-bottom:14px;">
                  <div style="width:64px;height:64px;border-radius:50%;background-color:#fff4e6;display:inline-flex;align-items:center;justify-content:center;font-size:28px;line-height:1;color:#f57c00;">
                    🔓
                  </div>
                </div>

                <!-- Title -->
                <h2 style="font-size:22px;font-weight:600;color:#7b0f0f;margin:6px 0 12px 0;">Google Account Disconnected</h2>

                <!-- Greeting -->
                <p style="font-size:15px;color:#333;margin:6px 0 8px 0;">Hi <strong>${username}</strong>,</p>

                <!-- Main message -->
                <p style="font-size:14px;color:#555;line-height:1.6;margin:0 0 18px 0;">
                  Your Google account has been successfully disconnected from your Fotosfolio account.
                </p>

                <!-- Disconnection Details Box -->
                ${
                  datetime || ipAddress || location || device
                    ? `
                <div style="margin:20px auto;max-width:480px;background-color:#f9f9f9;border:1px solid #e8e8e8;border-radius:8px;padding:16px 20px;text-align:left;">
                  <p style="font-size:13px;font-weight:600;color:#333;margin:0 0 10px 0;">Disconnection Details:</p>
                  ${datetime ? `<p style="font-size:13px;color:#666;margin:4px 0;"><strong>Time:</strong> ${formattedDate}</p>` : ''}
                  ${ipAddress ? `<p style="font-size:13px;color:#666;margin:4px 0;"><strong>IP Address:</strong> ${ipAddress}</p>` : ''}
                  ${location ? `<p style="font-size:13px;color:#666;margin:4px 0;"><strong>Location:</strong> ${location}</p>` : ''}
                  ${device ? `<p style="font-size:13px;color:#666;margin:4px 0;"><strong>Device:</strong> ${device}</p>` : ''}
                </div>
                `
                    : ''
                }

                <!-- Information message -->
                <p style="font-size:14px;color:#555;line-height:1.6;margin:18px 0;">
                  You can now log in using your email and password.<br />
                  If you previously only used Google to sign in, please make sure to set up a password for your account.
                </p>

                <!-- CTA button -->
                <div style="margin:18px 0 10px 0;">
                  <a href="https://fotosfolio.com/settings"
                     style="display:inline-block;background-color:#7b0f0f;color:#ffffff;text-decoration:none;padding:12px 28px;border-radius:25px;font-weight:500;font-size:15px;box-shadow:0 2px 8px rgba(179,32,32,0.15);">
                    Go to Account Settings
                  </a>
                </div>

                <!-- Warning -->
                <div style="margin-top:20px;max-width:520px;margin-left:auto;margin-right:auto;background-color:#fdf6f6;border:1px solid #f0dcdc;border-radius:8px;padding:12px 14px;text-align:center;color:#7a1217;font-size:13px;line-height:1.5;">
                  ⚠️ If you didn't disconnect your Google account, please
                  <a href="https://fotosfolio.com/contact-us" style="color:#7b0f0f;text-decoration:none;">contact our support team</a>
                  immediately to secure your account.
                </div>

              </td>
            </tr>

            <!-- Footer -->
            <tr>
              <td style="padding:20px 10px; border-top:1px solid #eee; background-color:#fafafa; text-align:center;">
                <p style="font-size:12px; color:#999; margin:8px 0;">© ${this.getCurrentYear()} Fotosfolio. All rights reserved.</p>
                <p style="font-size:12px; color:#999; margin:0;">
                  <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
                  <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
                  <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
                </p>
              </td>
            </tr>

          </table>
        </td>
      </tr>
    </table>

    </body>
    </html>
    `;
    console.log('Google account disconnected email sent successfully');
    return { to, subject, text, html };
  }
}
