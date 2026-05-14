// src/email-services/security-emails.service.ts
import { Injectable } from '@nestjs/common';
import { BaseEmailService, EmailData } from './base-email.service';

@Injectable()
export class SecurityEmailsService extends BaseEmailService {
  
  async buildLoginNotificationEmail(
    to: string,
    userName: string,
    loginTime: Date | string,
    ipAddress: string
  ): Promise<EmailData> {
    const subject = 'New Login to Your Fotosfolio Account';
    // Handle both Date object and string (string comes from queue serialization)
    const formattedDate = typeof loginTime === 'string' ? loginTime : loginTime.toISOString();

    const text = `Hi ${userName},

We noticed a new login to your Fotosfolio account.

Time: ${formattedDate}
IP Address: ${ipAddress}

If this was you, you can safely ignore this message.

If you didn't authorize this login, please reset your password immediately.

– Fotosfolio Team`;

    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding-right:12px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td>
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block; border:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Email Body -->
        <div style="padding:30px;text-align:center;">
          <h2 style="font-size:20px;color:#111;margin-bottom:12px;">New Login to Your Fotosfolio Account</h2>

          <p style="color:#555;font-size:15px;margin-bottom:20px;">
            Hi <strong>${userName}</strong>,
          </p>

          <p style="color:#555;font-size:15px;line-height:1.6;margin-bottom:25px;">
            We noticed a new login to your Fotosfolio account.
          </p>

          <!-- Highlight Box -->
          <div style="background-color:#fff8e6;border-left:3px solid #f59e0b;border-radius:8px;padding:16px 18px;margin:0 auto 24px;max-width:90%;">
            <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">Login Details</h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr>
                <td style="padding:6px 0;color:#666;">Time:</td>
                <td style="text-align:right;color:#444;">${formattedDate}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">IP Address:</td>
                <td style="text-align:right;color:#444;">${ipAddress}</td>
              </tr>
            </table>
          </div>

          <!-- Divider -->
          <div style="margin:20px 0;">
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
            <span style="display:inline-block;width:6px;height:6px;background:#8b0d0d;border-radius:50%;margin:0 6px;vertical-align:middle;"></span>
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
          </div>

          <p style="color:#555;font-size:14px;margin-bottom:8px;">
            If this was you, you can safely ignore this message.
          </p>

          <p style="color:#555;font-size:14px;margin-bottom:25px;">
            If you didn't authorize this login, please reset your password immediately.
          </p>

          <!-- Call-to-Action Button -->
          <a href="https://fotosfolio.com/reset-password" target="_blank"
             style="display:inline-block;background-color:#8b0d0d;color:#fff;text-decoration:none;
             padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;margin-bottom:35px;">
            Reset Password
          </a>

          <!-- Assistance Box -->
          <div style="background-color:#fafafa;border:1px solid #e2e2e2;border-radius:8px;padding:18px;margin-top:15px;">
            <p style="font-size:14px;color:#555;margin:0;">
              We take your account security seriously. If you need assistance, please
              <a href="https://fotosfolio.com/contact" style="color:#8b0d0d;text-decoration:none;">contact us</a>.
            </p>
          </div>

          <p style="margin-top:40px;color:#444;font-size:14px;">Best regards,</p>
          <p style="font-weight:600;color:#111;">– Fotosfolio Team</p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>
    `;
    return { to, subject, text, html };
  }

  async buildNewIpLoginAlertEmail(
    userEmail: string,
    ipAddress: string,
    device: string,
    location: string,
    datetime: Date,
  ): Promise<EmailData> {
    const subject = `New Device Login Detected`;
    const text = `We detected a new login to your Fotosfolio account.

  Device: ${device}
  Location: ${location}
  IP Address: ${ipAddress}
  Time: ${datetime}

  If this wasn’t you, please reset your password immediately:
  https://fotosfolio.com/reset-password

  – The Fotosfolio Team`;

    const html = `
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <title>New Device Login Alert</title>
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
                <h2 style="font-size:22px; font-weight:600; color:#7b0f0f; margin:0 0 10px;">New Device Login Detected</h2>
                <p style="color:#555; font-size:14px; margin:0 0 25px;">We detected a new login to your Fotosfolio account:</p>

                <!-- Details Table -->
                <table width="100%" role="presentation" style="background-color:#fafafa; border:1px solid #e0e0e0; border-radius:8px; padding:16px 20px; font-size:14px; color:#333;">
                  <tr>
                    <td style="padding:8px 0; color:#777;">Device:</td>
                    <td style="text-align:right;">${device}</td>
                  </tr>
                  <tr>
                    <td style="padding:8px 0; color:#777;">Location:</td>
                    <td style="text-align:right;">${location}</td>
                  </tr>
                  <tr>
                    <td style="padding:8px 0; color:#777;">IP Address:</td>
                    <td style="text-align:right;">${ipAddress}</td>
                  </tr>
                  <tr>
                    <td style="padding:8px 0; color:#777;">Time:</td>
                    <td style="text-align:right;">${datetime}</td>
                  </tr>
                </table>

                <p style="font-size:14px; color:#7b0f0f; text-align:center; margin:28px 0 14px;">
                  If this wasn’t you, please secure your account immediately.
                </p>

                <div style="text-align:center; margin:25px 0 35px 0;">
                  <a href="https://fotosfolio.com/reset-password"
                    style="display:inline-block; background-color:#7b0f0f; color:#ffffff; text-decoration:none;
                    padding:12px 30px; border-radius:25px; font-weight:500; font-size:15px; box-shadow:0 2px 8px rgba(179,32,32,0.15);">
                    Reset Password
                  </a>
                </div>

                <p style="text-align:center; font-size:13px; color:#555; line-height:1.5; margin:0;">
                  We care about your security and are here to help if anything seems suspicious.
                  <br><a href="https://fotosfolio.com/contact-us" style="color:#b32020; text-decoration:none;">Contact Support</a>
                </p>
              </td>
            </tr>

            <!-- Footer -->
            <tr>
              <td style="padding:20px 10px; border-top:1px solid #eee; background-color:#fafafa; text-align:center;">
                <p style="font-size:12px; color:#999; margin:8px 0;">© ${new Date().getFullYear()} Fotosfolio. All rights reserved.</p>
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
    return { to: userEmail, subject, text, html };
  }

  async build2FAEnabledEmail(
    userEmail: string,
    device: string,
    location: string,
    ipAddress: string,
    datetime: Date
  ): Promise<EmailData> {
    const subject = `Two-Factor Authentication Enabled`;
    const text = `Two-factor authentication has been enabled on your Fotosfolio account.

  Device: ${device}
  Location: ${location}
  IP Address: ${ipAddress}
  Time: ${datetime}

  If this wasn't you, please secure your account immediately:
  https://fotosfolio.com/reset-password

  – The Fotosfolio Team`;

    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding-right:12px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td>
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block; border:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Email Body -->
        <div style="padding:30px;text-align:center;">
          <h2 style="font-size:20px;color:#111;margin-bottom:12px;">Two-Factor Authentication Enabled</h2>

          <p style="color:#555;font-size:15px;margin-bottom:20px;">
            Your account security has been enhanced.
          </p>

          <p style="color:#555;font-size:15px;line-height:1.6;margin-bottom:25px;">
            Two-factor authentication (2FA) has been successfully enabled on your Fotosfolio account. This adds an extra layer of security to your login process.
          </p>

          <!-- Highlight Box -->
          <div style="background-color:#eaf8ea;border-left:3px solid #0d8b0d;border-radius:8px;padding:16px 18px;margin:0 auto 24px;max-width:90%;">
            <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">Action Details</h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr>
                <td style="padding:6px 0;color:#666;">Device:</td>
                <td style="text-align:right;color:#444;">${device}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Location:</td>
                <td style="text-align:right;color:#444;">${location}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">IP Address:</td>
                <td style="text-align:right;color:#444;">${ipAddress}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Time:</td>
                <td style="text-align:right;color:#444;">${datetime}</td>
              </tr>
            </table>
          </div>

          <!-- Divider -->
          <div style="margin:20px 0;">
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
            <span style="display:inline-block;width:6px;height:6px;background:#8b0d0d;border-radius:50%;margin:0 6px;vertical-align:middle;"></span>
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
          </div>

          <p style="color:#555;font-size:14px;margin-bottom:25px;">
            If this wasn't you, please secure your account immediately:
          </p>

          <!-- Call-to-Action Button -->
          <a href="https://fotosfolio.com/reset-password" target="_blank"
             style="display:inline-block;background-color:#8b0d0d;color:#fff;text-decoration:none;
             padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;margin-bottom:35px;">
            Secure Account
          </a>

          <!-- Assistance Box -->
          <div style="background-color:#fafafa;border:1px solid #e2e2e2;border-radius:8px;padding:18px;margin-top:15px;">
            <p style="font-size:14px;color:#555;margin:0;">
              We care about your security and are here to help if anything seems suspicious. Please
              <a href="https://fotosfolio.com/contact" style="color:#8b0d0d;text-decoration:none;">contact us</a> if you need assistance.
            </p>
          </div>

          <p style="margin-top:40px;color:#444;font-size:14px;">Stay secure,</p>
          <p style="font-weight:600;color:#111;">The FotosFolio Team</p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>
  `;
    console.log('2FA enabled email sent successfully');
    return { to: userEmail, subject, text, html };
  }

  async build2FADisabledEmail(
    userEmail: string,
    device: string,
    location: string,
    ipAddress: string,
    datetime: Date
  ): Promise<EmailData> {
    const subject = `Two-Factor Authentication Disabled`;
    const text = `Two-factor authentication has been disabled on your Fotosfolio account.

  Device: ${device}
  Location: ${location}
  IP Address: ${ipAddress}
  Time: ${datetime}

  If this wasn't you, please secure your account immediately:
  https://fotosfolio.com/reset-password

  – The Fotosfolio Team`;

    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding-right:12px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td>
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block; border:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Email Body -->
        <div style="padding:30px;text-align:center;">
          <h2 style="font-size:20px;color:#111;margin-bottom:12px;">Two-Factor Authentication Disabled</h2>

          <p style="color:#555;font-size:15px;margin-bottom:20px;">
            Your account security settings have changed.
          </p>

          <p style="color:#555;font-size:15px;line-height:1.6;margin-bottom:25px;">
            Two-factor authentication (2FA) has been disabled on your Fotosfolio account. Your account is now less secure.
          </p>

          <!-- Highlight Box -->
          <div style="background-color:#f8eaea;border-left:3px solid #8b0d0d;border-radius:8px;padding:16px 18px;margin:0 auto 24px;max-width:90%;">
            <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">Action Details</h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr>
                <td style="padding:6px 0;color:#666;">Device:</td>
                <td style="text-align:right;color:#444;">${device}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Location:</td>
                <td style="text-align:right;color:#444;">${location}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">IP Address:</td>
                <td style="text-align:right;color:#444;">${ipAddress}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Time:</td>
                <td style="text-align:right;color:#444;">${datetime}</td>
              </tr>
            </table>
          </div>

          <!-- Divider -->
          <div style="margin:20px 0;">
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
            <span style="display:inline-block;width:6px;height:6px;background:#8b0d0d;border-radius:50%;margin:0 6px;vertical-align:middle;"></span>
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
          </div>

          <p style="color:#555;font-size:14px;margin-bottom:25px;">
            If this wasn't you, please secure your account immediately:
          </p>

          <!-- Call-to-Action Button -->
          <a href="https://fotosfolio.com/reset-password" target="_blank"
             style="display:inline-block;background-color:#8b0d0d;color:#fff;text-decoration:none;
             padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;margin-bottom:35px;">
            Reset Password
          </a>

          <!-- Assistance Box -->
          <div style="background-color:#fafafa;border:1px solid #e2e2e2;border-radius:8px;padding:18px;margin-top:15px;">
            <p style="font-size:14px;color:#555;margin:0;">
              We care about your security and are here to help if anything seems suspicious. Please
              <a href="https://fotosfolio.com/contact" style="color:#8b0d0d;text-decoration:none;">contact us</a> if you need assistance.
            </p>
          </div>

          <p style="margin-top:40px;color:#444;font-size:14px;">Stay secure,</p>
          <p style="font-weight:600;color:#111;">The FotosFolio Team</p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>
    `;
    console.log('2FA disabled email sent successfully');
    return { to: userEmail, subject, text, html };
  }

  async buildPasskeyEnabledEmail(
    userEmail: string,
    passkeyName: string,
    device: string,
    location: string,
    ipAddress: string,
    datetime: Date
  ): Promise<EmailData> {
    const subject = `Passkey Added to Your Account`;
    const text = `A new passkey has been added to your Fotosfolio account.

  Passkey Name: ${passkeyName}
  Device: ${device}
  Location: ${location}
  IP Address: ${ipAddress}
  Time: ${datetime}

  If this wasn't you, please secure your account immediately:
  https://fotosfolio.com/reset-password

  – The Fotosfolio Team`;

    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding-right:12px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td>
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block; border:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Email Body -->
        <div style="padding:30px;text-align:center;">
          <h2 style="font-size:20px;color:#111;margin-bottom:12px;">Passkey Added Successfully</h2>

          <p style="color:#555;font-size:15px;margin-bottom:20px;">
            A new passkey has been registered to your account.
          </p>

          <p style="color:#555;font-size:15px;line-height:1.6;margin-bottom:25px;">
            A new passkey has been added to your Fotosfolio account. You can now use this passkey for secure, passwordless authentication.
          </p>

          <!-- Highlight Box -->
          <div style="background-color:#eaf8ea;border-left:3px solid #0d8b0d;border-radius:8px;padding:16px 18px;margin:0 auto 24px;max-width:90%;">
            <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">Passkey Details</h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr>
                <td style="padding:6px 0;color:#666;">Passkey Name:</td>
                <td style="text-align:right;color:#444;">${passkeyName}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Device:</td>
                <td style="text-align:right;color:#444;">${device}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Location:</td>
                <td style="text-align:right;color:#444;">${location}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">IP Address:</td>
                <td style="text-align:right;color:#444;">${ipAddress}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Time:</td>
                <td style="text-align:right;color:#444;">${datetime}</td>
              </tr>
            </table>
          </div>

          <!-- Divider -->
          <div style="margin:20px 0;">
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
            <span style="display:inline-block;width:6px;height:6px;background:#8b0d0d;border-radius:50%;margin:0 6px;vertical-align:middle;"></span>
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
          </div>

          <p style="color:#555;font-size:14px;margin-bottom:25px;">
            If this wasn't you, please secure your account immediately:
          </p>

          <!-- Call-to-Action Button -->
          <a href="https://fotosfolio.com/reset-password" target="_blank"
             style="display:inline-block;background-color:#8b0d0d;color:#fff;text-decoration:none;
             padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;margin-bottom:35px;">
            Secure Account
          </a>

          <!-- Assistance Box -->
          <div style="background-color:#fafafa;border:1px solid #e2e2e2;border-radius:8px;padding:18px;margin-top:15px;">
            <p style="font-size:14px;color:#555;margin:0;">
              We care about your security and are here to help if anything seems suspicious. Please
              <a href="https://fotosfolio.com/contact" style="color:#8b0d0d;text-decoration:none;">contact us</a> if you need assistance.
            </p>
          </div>

          <p style="margin-top:40px;color:#444;font-size:14px;">Stay secure,</p>
          <p style="font-weight:600;color:#111;">The FotosFolio Team</p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>
    `;
    console.log('Passkey enabled email sent successfully');
    return { to: userEmail, subject, text, html };
  }

  async buildPasskeyDisabledEmail(
    userEmail: string,
    passkeyName: string,
    device: string,
    location: string,
    ipAddress: string,
    datetime: Date
  ): Promise<EmailData> {
    const subject = `Passkey Removed from Your Account`;
    const text = `A passkey has been removed from your Fotosfolio account.

  Passkey Name: ${passkeyName}
  Device: ${device}
  Location: ${location}
  IP Address: ${ipAddress}
  Time: ${datetime}

  If this wasn't you, please secure your account immediately:
  https://fotosfolio.com/reset-password

  – The Fotosfolio Team`;

    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding-right:12px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td>
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block; border:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Email Body -->
        <div style="padding:30px;text-align:center;">
          <h2 style="font-size:20px;color:#111;margin-bottom:12px;">Passkey Removed</h2>

          <p style="color:#555;font-size:15px;margin-bottom:20px;">
            A passkey has been removed from your account.
          </p>

          <p style="color:#555;font-size:15px;line-height:1.6;margin-bottom:25px;">
            A passkey has been removed from your Fotosfolio account. You can no longer use this passkey for authentication.
          </p>

          <!-- Highlight Box -->
          <div style="background-color:#f8eaea;border-left:3px solid #8b0d0d;border-radius:8px;padding:16px 18px;margin:0 auto 24px;max-width:90%;">
            <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">Passkey Details</h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr>
                <td style="padding:6px 0;color:#666;">Passkey Name:</td>
                <td style="text-align:right;color:#444;">${passkeyName}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Device:</td>
                <td style="text-align:right;color:#444;">${device}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Location:</td>
                <td style="text-align:right;color:#444;">${location}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">IP Address:</td>
                <td style="text-align:right;color:#444;">${ipAddress}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Time:</td>
                <td style="text-align:right;color:#444;">${datetime}</td>
              </tr>
            </table>
          </div>

          <!-- Divider -->
          <div style="margin:20px 0;">
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
            <span style="display:inline-block;width:6px;height:6px;background:#8b0d0d;border-radius:50%;margin:0 6px;vertical-align:middle;"></span>
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
          </div>

          <p style="color:#555;font-size:14px;margin-bottom:25px;">
            If this wasn't you, please secure your account immediately:
          </p>

          <!-- Call-to-Action Button -->
          <a href="https://fotosfolio.com/reset-password" target="_blank"
             style="display:inline-block;background-color:#8b0d0d;color:#fff;text-decoration:none;
             padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;margin-bottom:35px;">
            Reset Password
          </a>

          <!-- Assistance Box -->
          <div style="background-color:#fafafa;border:1px solid #e2e2e2;border-radius:8px;padding:18px;margin-top:15px;">
            <p style="font-size:14px;color:#555;margin:0;">
              We care about your security and are here to help if anything seems suspicious. Please
              <a href="https://fotosfolio.com/contact" style="color:#8b0d0d;text-decoration:none;">contact us</a> if you need assistance.
            </p>
          </div>

          <p style="margin-top:40px;color:#444;font-size:14px;">Stay secure,</p>
          <p style="font-weight:600;color:#111;">The FotosFolio Team</p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>
    `;
    console.log('Passkey disabled email sent successfully');
    return { to: userEmail, subject, text, html };
  }

  async buildSecurityQuestionEnabledEmail(
    userEmail: string,
    device: string,
    location: string,
    ipAddress: string,
    datetime: Date
  ): Promise<EmailData> {
    const subject = `Security Questions Configured`;
    const text = `Security questions have been set up on your Fotosfolio account.

  Device: ${device}
  Location: ${location}
  IP Address: ${ipAddress}
  Time: ${datetime}

  If this wasn't you, please secure your account immediately:
  https://fotosfolio.com/reset-password

  – The Fotosfolio Team`;

    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding-right:12px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td>
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block; border:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Email Body -->
        <div style="padding:30px;text-align:center;">
          <h2 style="font-size:20px;color:#111;margin-bottom:12px;">Security Questions Configured</h2>

          <p style="color:#555;font-size:15px;margin-bottom:20px;">
            Additional account recovery options have been set up.
          </p>

          <p style="color:#555;font-size:15px;line-height:1.6;margin-bottom:25px;">
            Security questions have been successfully configured on your Fotosfolio account. These can be used to help verify your identity during account recovery.
          </p>

          <!-- Highlight Box -->
          <div style="background-color:#eaf8ea;border-left:3px solid #0d8b0d;border-radius:8px;padding:16px 18px;margin:0 auto 24px;max-width:90%;">
            <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">Action Details</h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr>
                <td style="padding:6px 0;color:#666;">Device:</td>
                <td style="text-align:right;color:#444;">${device}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Location:</td>
                <td style="text-align:right;color:#444;">${location}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">IP Address:</td>
                <td style="text-align:right;color:#444;">${ipAddress}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Time:</td>
                <td style="text-align:right;color:#444;">${datetime}</td>
              </tr>
            </table>
          </div>

          <!-- Divider -->
          <div style="margin:20px 0;">
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
            <span style="display:inline-block;width:6px;height:6px;background:#8b0d0d;border-radius:50%;margin:0 6px;vertical-align:middle;"></span>
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
          </div>

          <p style="color:#555;font-size:14px;margin-bottom:25px;">
            If this wasn't you, please secure your account immediately:
          </p>

          <!-- Call-to-Action Button -->
          <a href="https://fotosfolio.com/reset-password" target="_blank"
             style="display:inline-block;background-color:#8b0d0d;color:#fff;text-decoration:none;
             padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;margin-bottom:35px;">
            Secure Account
          </a>

          <!-- Assistance Box -->
          <div style="background-color:#fafafa;border:1px solid #e2e2e2;border-radius:8px;padding:18px;margin-top:15px;">
            <p style="font-size:14px;color:#555;margin:0;">
              We care about your security and are here to help if anything seems suspicious. Please
              <a href="https://fotosfolio.com/contact" style="color:#8b0d0d;text-decoration:none;">contact us</a> if you need assistance.
            </p>
          </div>

          <p style="margin-top:40px;color:#444;font-size:14px;">Stay secure,</p>
          <p style="font-weight:600;color:#111;">The FotosFolio Team</p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>
    `;
    console.log('Security question enabled email sent successfully');
    return { to: userEmail, subject, text, html };
  }

  async buildSecurityQuestionDisabledEmail(
    userEmail: string,
    device: string,
    location: string,
    ipAddress: string,
    datetime: Date
  ): Promise<EmailData> {
    const subject = `Security Questions Removed`;
    const text = `Security questions have been removed from your Fotosfolio account.

  Device: ${device}
  Location: ${location}
  IP Address: ${ipAddress}
  Time: ${datetime}

  If this wasn't you, please secure your account immediately:
  https://fotosfolio.com/reset-password

  – The Fotosfolio Team`;

    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding-right:12px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block; border:none;" />
                    </a>
                  </td>
                  <td>
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block; border:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Email Body -->
        <div style="padding:30px;text-align:center;">
          <h2 style="font-size:20px;color:#111;margin-bottom:12px;">Security Questions Removed</h2>

          <p style="color:#555;font-size:15px;margin-bottom:20px;">
            Account recovery options have changed.
          </p>

          <p style="color:#555;font-size:15px;line-height:1.6;margin-bottom:25px;">
            Security questions have been removed from your Fotosfolio account. These can no longer be used for account recovery.
          </p>

          <!-- Highlight Box -->
          <div style="background-color:#f8eaea;border-left:3px solid #8b0d0d;border-radius:8px;padding:16px 18px;margin:0 auto 24px;max-width:90%;">
            <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">Action Details</h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr>
                <td style="padding:6px 0;color:#666;">Device:</td>
                <td style="text-align:right;color:#444;">${device}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Location:</td>
                <td style="text-align:right;color:#444;">${location}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">IP Address:</td>
                <td style="text-align:right;color:#444;">${ipAddress}</td>
              </tr>
              <tr>
                <td style="padding:6px 0;color:#666;">Time:</td>
                <td style="text-align:right;color:#444;">${datetime}</td>
              </tr>
            </table>
          </div>

          <!-- Divider -->
          <div style="margin:20px 0;">
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
            <span style="display:inline-block;width:6px;height:6px;background:#8b0d0d;border-radius:50%;margin:0 6px;vertical-align:middle;"></span>
            <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
          </div>

          <p style="color:#555;font-size:14px;margin-bottom:25px;">
            If this wasn't you, please secure your account immediately:
          </p>

          <!-- Call-to-Action Button -->
          <a href="https://fotosfolio.com/reset-password" target="_blank"
             style="display:inline-block;background-color:#8b0d0d;color:#fff;text-decoration:none;
             padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;margin-bottom:35px;">
            Reset Password
          </a>

          <!-- Assistance Box -->
          <div style="background-color:#fafafa;border:1px solid #e2e2e2;border-radius:8px;padding:18px;margin-top:15px;">
            <p style="font-size:14px;color:#555;margin:0;">
              We care about your security and are here to help if anything seems suspicious. Please
              <a href="https://fotosfolio.com/contact" style="color:#8b0d0d;text-decoration:none;">contact us</a> if you need assistance.
            </p>
          </div>

          <p style="margin-top:40px;color:#444;font-size:14px;">Stay secure,</p>
          <p style="font-weight:600;color:#111;">The FotosFolio Team</p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>
    `;
    console.log('Security question disabled email sent successfully');
    return { to: userEmail, subject, text, html };
  }
}
