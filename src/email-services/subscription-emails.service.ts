// src/email-services/subscription-emails.service.ts
import { Injectable } from '@nestjs/common';
import { BaseEmailService, EmailData } from './base-email.service';

@Injectable()
export class SubscriptionEmailsService extends BaseEmailService {
  
  async buildSubscriptionCreatedEmail(
    to: string,
    userName: string,
    planName: string,
    startDate: Date,
    endDate: Date,
    status: string
  ): Promise<EmailData> {
    const subject = 'Your Fotosfolio Subscription is Now Active!';
    const text = `Hi ${userName},

  Your ${planName} subscription has been successfully activated.

  Start Date: ${new Date(startDate).toLocaleDateString()}
  End Date: ${new Date(endDate).toLocaleDateString()}
  Status: ${status}

  Thank you for choosing Fotosfolio.

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
          <div style="font-size:34px;color:#7a1217;margin-bottom:8px;">🎉</div>
          <h2 style="color:#7a1217;font-size:22px;font-weight:600;margin:0;">Your Subscription is Active!</h2>
          <p style="margin-top:6px;color:#555;font-size:14px;">Your Fotosfolio plan is now live.</p>
        </div>

        <!-- Body -->
        <div style="padding:0 40px 30px 40px;text-align:left;box-sizing:border-box;">
          <p style="font-size:15px;color:#333;margin:0 0 10px 0;">Hi ${userName},</p>
          <p style="font-size:14px;color:#555;margin:0 0 18px 0;">
            Your <strong style="color:#7a1217;">${planName}</strong> subscription has been successfully activated.
          </p>
          <p style="font-size:14px;color:#555;margin-bottom:25px;">
            Thank you for trusting Fotosfolio to store, manage, and share your files securely and efficiently.
          </p>

          <!-- Subscription Details -->
          <div style="border:1px solid #e8e8e8;border-radius:10px;background:#fff;width:100%;margin:0 auto 25px;box-sizing:border-box;">
            <div style="padding:18px 25px;box-sizing:border-box;">
              <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">Subscription Details</h4>
              <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;box-sizing:border-box;">
                <tr style="border-bottom:1px solid #f3f3f3;">
                  <td style="padding:8px 0;color:#777;">Plan:</td>
                  <td style="text-align:right;font-weight:500;">${planName}</td>
                </tr>
                <tr style="border-bottom:1px solid #f3f3f3;">
                  <td style="padding:8px 0;color:#777;">Start Date:</td>
                  <td style="text-align:right;font-weight:500;">${new Date(startDate).toLocaleDateString()}</td>
                </tr>
                <tr style="border-bottom:1px solid #f3f3f3;">
                  <td style="padding:8px 0;color:#777;">End Date:</td>
                  <td style="text-align:right;font-weight:500;">${new Date(endDate).toLocaleDateString()}</td>
                </tr>
                <tr>
                  <td style="padding:8px 0;color:#777;">Status:</td>
                  <td style="text-align:right;font-weight:600;color:#7a1217;">${status}</td>
                </tr>
              </table>
            </div>
          </div>

          <!-- Button -->
          <div style="text-align:center;margin:30px 0 25px 0;">
            <a href="https://fotosfolio.com/account" style="
              display:inline-block;
              background-color:#7a1217;
              color:#fff;
              text-decoration:none;
              padding:12px 28px;
              border-radius:25px;
              font-size:15px;
              font-weight:500;">
              Go to My Account
            </a>
          </div>

          <!-- Help Box -->
          <div style="background-color:#faf9f9;border:1px solid #f2f2f2;border-radius:10px;text-align:center;padding:20px 25px;font-size:13px;color:#555;margin-bottom:25px;box-sizing:border-box;">
            <strong>Need Assistance?</strong><br />
            Manage billing or upgrade anytime.<br />
            For help, <a href="mailto:support@fotosfolio.com" style="color:#7a1217;text-decoration:none;">contact us</a> or visit our
            <a href="https://fotosfolio.com/help" style="color:#7a1217;text-decoration:none;">Help Center</a>.
          </div>

          <!-- Quote -->
          <p style="text-align:center;margin:30px 0 10px 0;font-size:14px;color:#7a1217;font-style:italic;">
            “Empowering you to manage your files with confidence and simplicity.”
          </p>

          <!-- Signature -->
          <p style="text-align:center;color:#333;font-size:14px;font-weight:600;margin-bottom:0;">
            — The Fotosfolio Team
          </p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px 10px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
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

  async buildSubscriptionRenewedEmail(
    to: string,
    userName: string,
    planName: string,
    startDate: Date,
    endDate: Date,
    status: string
  ): Promise<EmailData> {
    const subject = 'Your Fotosfolio Subscription Has Been Renewed!';
    const text = `Hi ${userName},

  Your ${planName} subscription has been successfully renewed.

  Renewal Date: ${new Date(startDate).toLocaleDateString()}
  Next Renewal: ${new Date(endDate).toLocaleDateString()}
  Status: ${status}

  Thank you for continuing with Fotosfolio.

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
          <div style="font-size:34px;color:#7a1217;margin-bottom:8px;">🔄</div>
          <h2 style="color:#7a1217;font-size:22px;font-weight:600;margin:0;">Subscription Renewed!</h2>
          <p style="margin-top:6px;color:#555;font-size:14px;">Your Fotosfolio plan has been renewed.</p>
        </div>

        <!-- Body -->
        <div style="padding:0 40px 30px 40px;text-align:left;box-sizing:border-box;">
          <p style="font-size:15px;color:#333;margin:0 0 10px 0;">Hi ${userName},</p>
          <p style="font-size:14px;color:#555;margin:0 0 18px 0;">
            Great news! Your <strong style="color:#7a1217;">${planName}</strong> subscription has been successfully renewed.
          </p>
          <p style="font-size:14px;color:#555;margin-bottom:25px;">
            Thank you for continuing to trust Fotosfolio to store, manage, and share your files securely.
          </p>

          <!-- Subscription Details -->
          <div style="border:1px solid #e8e8e8;border-radius:10px;background:#fff;width:100%;margin:0 auto 25px;box-sizing:border-box;">
            <div style="padding:18px 25px;box-sizing:border-box;">
              <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">Renewal Details</h4>
              <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;box-sizing:border-box;">
                <tr style="border-bottom:1px solid #f3f3f3;">
                  <td style="padding:8px 0;color:#777;">Plan:</td>
                  <td style="text-align:right;font-weight:500;">${planName}</td>
                </tr>
                <tr style="border-bottom:1px solid #f3f3f3;">
                  <td style="padding:8px 0;color:#777;">Renewal Date:</td>
                  <td style="text-align:right;font-weight:500;">${new Date(startDate).toLocaleDateString()}</td>
                </tr>
                <tr style="border-bottom:1px solid #f3f3f3;">
                  <td style="padding:8px 0;color:#777;">Next Renewal:</td>
                  <td style="text-align:right;font-weight:500;">${new Date(endDate).toLocaleDateString()}</td>
                </tr>
                <tr>
                  <td style="padding:8px 0;color:#777;">Status:</td>
                  <td style="text-align:right;font-weight:600;color:#7a1217;">${status}</td>
                </tr>
              </table>
            </div>
          </div>

          <!-- Button -->
          <div style="text-align:center;margin:30px 0 25px 0;">
            <a href="https://fotosfolio.com/account" style="
              display:inline-block;
              background-color:#7a1217;
              color:#fff;
              text-decoration:none;
              padding:12px 28px;
              border-radius:25px;
              font-size:15px;
              font-weight:500;">
              Go to My Account
            </a>
          </div>

          <!-- Help Box -->
          <div style="background-color:#faf9f9;border:1px solid #f2f2f2;border-radius:10px;text-align:center;padding:20px 25px;font-size:13px;color:#555;margin-bottom:25px;box-sizing:border-box;">
            <strong>Need Assistance?</strong><br />
            Manage your billing or view subscription details anytime.<br />
            For help, <a href="mailto:support@fotosfolio.com" style="color:#7a1217;text-decoration:none;">contact us</a> or visit our
            <a href="https://fotosfolio.com/help" style="color:#7a1217;text-decoration:none;">Help Center</a>.
          </div>

          <!-- Quote -->
          <p style="text-align:center;margin:30px 0 10px 0;font-size:14px;color:#7a1217;font-style:italic;">
            "Empowering you to manage your files with confidence and simplicity."
          </p>

          <!-- Signature -->
          <p style="text-align:center;color:#333;font-size:14px;font-weight:600;margin-bottom:0;">
            — The Fotosfolio Team
          </p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px 10px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
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

  async buildAccountActivationEmail(to: string, userName: string, planName: string): Promise<EmailData> {
   const subject = 'Your Account is Now Active! 🎉';

    const html = `
    <div style="font-family:'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background-color:#fafafa;padding:40px 0;">
      <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 14px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block;outline:none;text-decoration:none;border:none;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding:0 10px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block;border:none;" />
                    </a>
                  </td>
                  <td style="padding:0 10px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block;border:none;" />
                    </a>
                  </td>
                  <td style="padding:0 10px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block;border:none;" />
                    </a>
                  </td>
                  <td style="padding:0 0 0 10px;">
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
        <div style="text-align:center;padding:16px 25px 6px 25px;">
          <div style="font-size:36px;color:#7a1217;margin-bottom:10px;">🎉</div>
          <h2 style="color:#7a1217;font-size:22px;font-weight:600;margin:0;">Your Account is Now Active!</h2>
          <p style="margin-top:6px;color:#666;font-size:14px;">Welcome to Fotosfolio Premium</p>
        </div>

        <!-- Body -->
        <div style="padding:0 32px 20px 32px;text-align:left;">
          <p style="font-size:15px;color:#444;margin:0 0 10px 0;">Hi ${userName},</p>
          <p style="font-size:14px;color:#444;margin:0 0 18px 0;">
            Great news! Your <strong style="color:#7a1217;">${planName}</strong> plan is now active.
          </p>
          <p style="font-size:14px;color:#444;margin-bottom:25px;">
            You can now log in, explore your dashboard, and start managing your photos with ease.
          </p>

          <!-- Account Summary -->
          <div style="
            border:1px solid #e8e8e8;
            border-radius:10px;
            padding:18px 25px;
            background:#fff;
            max-width:100%;
            box-sizing:border-box;
            margin:0 auto;
          ">
            <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#444;">
              Account Details
            </h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#444;">
              <tr style="border-bottom:1px solid #f3f3f3;">
                <td style="padding:8px 0;color:#666;">Plan:</td>
                <td style="padding:8px 0;text-align:right;font-weight:500;">${planName}</td>
              </tr>
              <tr>
                <td style="padding:8px 0;color:#666;">Status:</td>
                <td style="padding:8px 0;text-align:right;font-weight:500;color:#7a1217;">Active</td>
              </tr>
            </table>
          </div>

          <!-- Button -->
          <div style="text-align:center;margin:35px 0 25px 0;">
            <a href="https://fotosfolio.com/dashboard" style="
              display:inline-block;
              background-color:#7a1217;
              color:#fff;
              text-decoration:none;
              padding:12px 28px;
              border-radius:25px;
              font-size:15px;
              font-weight:500;
            ">
              Go to Dashboard
            </a>
          </div>

          <!-- Help Box -->
          <div style="
            background-color:#faf9f9;
            border:1px solid #f2f2f2;
            border-radius:10px;
            text-align:center;
            padding:18px 25px;
            font-size:13px;
            color:#444;
            margin-top:20px;
          ">
            <strong>Need Help?</strong><br />
            Visit your dashboard to explore features, or contact us for any questions.<br />
            <a href="mailto:support@fotosfolio.com" style="color:#7a1217;text-decoration:none;">Email Support</a> or visit our
            <a href="https://fotosfolio.com/help" style="color:#7a1217;text-decoration:none;">Help Center</a>.
          </div>

          <!-- Quote -->
          <p style="text-align:center;margin:30px 0 10px 0;font-size:14px;color:#7a1217;font-style:italic;">
            “Empowering you to manage your photos with confidence and creativity.”
          </p>

          <!-- Signature -->
          <p style="text-align:center;color:#444;font-size:14px;font-weight:600;margin-bottom:10px;">
            — The Fotosfolio Team
          </p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7a1217;text-decoration:none;margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7a1217;text-decoration:none;margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7a1217;text-decoration:none;margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>
    `;
    const text = `
  Hi ${userName},

  Your ${planName} plan is now active!
  Log in and start exploring your Fotosfolio dashboard.

  – The Fotosfolio Team
    `;
    return { to, subject, text, html };
  }

  async buildSubscriptionExpirationReminderEmail(
    to: string,
    userName: string,
    daysRemaining: number
  ): Promise<EmailData> {
    const subject = 'Reminder: Your Subscription Will Expire Soon';

    const text = `Hi ${userName},

  Your Fotosfolio subscription will expire in ${daysRemaining} day(s).
  Please renew your plan to continue enjoying uninterrupted access to your files.

  – The Fotosfolio Team`;

    const html = `
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <meta http-equiv="x-ua-compatible" content="ie=edge" />
      <title>Subscription Expiration Reminder</title>
    </head>
    <body style="margin:0; padding:0; background-color:#f5f5f5; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">

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
                        <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; border:0; outline:none; text-decoration:none;" />
                      </td>

                      <!-- Social Icons -->
                      <td align="right" style="vertical-align:middle;">
                        <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                          <tr>
                            <td style="padding:0 4px;">
                              <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block;" />
                              </a>
                            </td>
                            <td style="padding:0 4px;">
                              <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block;" />
                              </a>
                            </td>
                            <td style="padding:0 4px;">
                              <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block;" />
                              </a>
                            </td>
                            <td style="padding:0 4px;">
                              <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block;" />
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
                <td style="padding:40px 36px; text-align:left;">

                  <h2 style="color:#7b0f0f; font-size:22px; font-weight:600; margin:0 0 8px 0;">
                    Your Subscription Is About to Expire
                  </h2>
                  <p style="font-size:14px; color:#555555; margin:0 0 20px 0;">
                    Hi ${userName}, your current Fotosfolio's plan  will expire soon.
                  </p>

                  <!-- Notice Box -->
                  <table width="100%" role="presentation" style="background-color:#fef3f3; border-left:4px solid #b32020; border-radius:8px; margin:25px 0; padding:0;">
                    <tr>
                      <td style="padding:16px 20px; font-size:14px; color:#b32020; line-height:1.6;">
                        Your subscription is set to expire in
                        <strong>${daysRemaining} day${Number(daysRemaining) > 1 ? 's' : ''}</strong>.
                        Please renew now to avoid any interruption in your account or file access.
                      </td>
                    </tr>
                  </table>

                  <p style="text-align:center; font-size:14px; color:#555; margin:25px 35px 0 35px;">
                    Renewing your subscription ensures continuous access to your storage and premium features without disruption.
                  </p>

                  <!-- Button -->
                  <div style="text-align:center; margin:32px 0 38px 0;">
                    <a href="https://fotosfolio.com/renew" style="
                      display:inline-block;
                      background-color:#7b0f0f;
                      color:#ffffff;
                      text-decoration:none;
                      padding:12px 30px;
                      border-radius:25px;
                      font-weight:500;
                      font-size:15px;
                      box-shadow:0 2px 8px rgba(179,32,32,0.15);
                    ">
                      Renew Subscription
                    </a>
                  </div>

                  <!-- Help Section -->
                  <table width="100%" role="presentation" style="background-color:#faf9f9; border:1px solid #f2f2f2; border-radius:10px; text-align:center; margin:0 0 20px 0;">
                    <tr>
                      <td style="padding:18px 25px; font-size:13px; color:#555;">
                        <strong>Need Help?</strong><br />
                        Our support team is here to assist you with renewal or payment issues.<br />
                        Visit our <a href="https://fotosfolio.com/help" style="color:#b32020; text-decoration:none;">Help Center</a>.
                      </td>
                    </tr>
                  </table>

                  <p style="text-align:center; color:#555; font-size:14px; margin:25px 0 10px 0;">
                    Thank you for being part of Fotosfolio.
                  </p>
                  <p style="text-align:center; color:#333; font-size:14px; font-weight:600; margin-bottom:20px;">
                    – The Fotosfolio Team
                  </p>

                </td>
              </tr>

              <!-- Footer -->
              <tr>
                <td style="border-top:1px solid #eaeaea; background-color:#fafafa; text-align:center; padding:15px; font-size:12px; color:#777;">
                  © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br />
                  <div style="margin-top:6px;">
                    <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
                    <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
                    <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a> •
                    <a href="https://fotosfolio.com/unsubscribe" style="color:#7b1717; text-decoration:none; margin:0 6px;">Unsubscribe</a>
                  </div>
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

  async buildSubscriptionExpiredEmail(
    to: string,
    userName: string,
    graceDaysRemaining: number
  ): Promise<EmailData> {
     const subject = 'Your Subscription Has Expired';

    const text = `Hi ${userName},

  Your Fotosfolio subscription has expired.

  Grace Period: You have ${graceDaysRemaining} day(s) remaining in your 3-day grace period.
  After this period, access to your account will be restricted.

  Please renew your plan to continue accessing your files and premium features.

  – The Fotosfolio Team`;

    const html = `
    <!doctype html>
    <html lang="en">
    <head>
      <meta charset="UTF-8" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <meta http-equiv="x-ua-compatible" content="ie=edge" />
      <title>Subscription Expired</title>
    </head>
    <body style="margin:0; padding:0; background-color:#f5f5f5; font-family:-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;">

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
                        <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; border:0; outline:none; text-decoration:none;" />
                      </td>

                      <!-- Social Icons -->
                      <td align="right" style="vertical-align:middle;">
                        <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                          <tr>
                            <td style="padding:0 4px;">
                              <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block;" />
                              </a>
                            </td>
                            <td style="padding:0 4px;">
                              <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block;" />
                              </a>
                            </td>
                            <td style="padding:0 4px;">
                              <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block;" />
                              </a>
                            </td>
                            <td style="padding:0 4px;">
                              <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                                <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block;" />
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
                <td style="padding:40px 36px; text-align:left;">

                  <h2 style="color:#7b0f0f; font-size:22px; font-weight:600; margin:0 0 8px 0;">
                    Your Subscription Has Expired
                  </h2>
                  <p style="font-size:14px; color:#555555; margin:0 0 20px 0;">
                    Hi ${userName}, your Fotosfolio subscription has expired.
                  </p>

                  <!-- Notice Box -->
                  <table width="100%" role="presentation" style="background-color:#fef3f3; border-left:4px solid #b32020; border-radius:8px; margin:25px 0; padding:0;">
                    <tr>
                      <td style="padding:16px 20px; font-size:14px; color:#b32020; line-height:1.6;">
                        Your subscription has expired. To continue accessing your storage and premium features, please renew your subscription now.
                      </td>
                    </tr>
                  </table>

                  <!-- Grace Period Notice -->
                  <table width="100%" role="presentation" style="background-color:#fff7e6; border-left:4px solid #ff9800; border-radius:8px; margin:25px 0; padding:0;">
                    <tr>
                      <td style="padding:16px 20px; font-size:14px; color:#e65100; line-height:1.6;">
                        <strong>Grace Period Active:</strong> You have <strong>${graceDaysRemaining} day${graceDaysRemaining !== 1 ? 's' : ''}</strong> remaining in your 3-day grace period.
                        After this period ends, your account access will be restricted until renewal.
                      </td>
                    </tr>
                  </table>

                  <p style="text-align:center; font-size:14px; color:#555; margin:25px 35px 0 35px;">
                    Renewing your subscription will restore full access to all your files and features immediately.
                  </p>

                  <!-- Button -->
                  <div style="text-align:center; margin:32px 0 38px 0;">
                    <a href="https://fotosfolio.com/renew" style="
                      display:inline-block;
                      background-color:#7b0f0f;
                      color:#ffffff;
                      text-decoration:none;
                      padding:12px 30px;
                      border-radius:25px;
                      font-weight:500;
                      font-size:15px;
                      box-shadow:0 2px 8px rgba(179,32,32,0.15);
                    ">
                      Renew Subscription Now
                    </a>
                  </div>

                  <!-- Help Section -->
                  <table width="100%" role="presentation" style="background-color:#faf9f9; border:1px solid #f2f2f2; border-radius:10px; text-align:center; margin:0 0 20px 0;">
                    <tr>
                      <td style="padding:18px 25px; font-size:13px; color:#555;">
                        <strong>Need Help?</strong><br />
                        Our support team is here to assist you with renewal or payment issues.<br />
                        Visit our <a href="https://fotosfolio.com/help" style="color:#b32020; text-decoration:none;">Help Center</a>.
                      </td>
                    </tr>
                  </table>

                  <p style="text-align:center; color:#555; font-size:14px; margin:25px 0 10px 0;">
                    Thank you for being part of Fotosfolio.
                  </p>
                  <p style="text-align:center; color:#333; font-size:14px; font-weight:600; margin-bottom:20px;">
                    – The Fotosfolio Team
                  </p>

                </td>
              </tr>

              <!-- Footer -->
              <tr>
                <td style="border-top:1px solid #eaeaea; background-color:#fafafa; text-align:center; padding:15px; font-size:12px; color:#777;">
                  © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br />
                  <div style="margin-top:6px;">
                    <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
                    <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
                    <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a> •
                    <a href="https://fotosfolio.com/unsubscribe" style="color:#7b1717; text-decoration:none; margin:0 6px;">Unsubscribe</a>
                  </div>
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
}
