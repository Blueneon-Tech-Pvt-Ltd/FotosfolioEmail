// src/email-services/payment-emails.service.ts
import { Injectable } from '@nestjs/common';
import { BaseEmailService } from './base-email.service';
import { EmailData, EmailCategory, EmailType } from '../types/email-types';

@Injectable()
export class PaymentEmailsService extends BaseEmailService {
  /**
   * Build PAYMENT_SUCCESS email
   */
  async buildPaymentSuccess(payload: any): Promise<EmailData> {
    const { to, userName, amount, transactionId, planName } = payload;
    const subject = 'Payment Successful – Thank You!';

    const text = `Hi ${userName},

Your payment of $${amount} has been processed successfully.

Transaction ID: ${transactionId}
Plan: ${planName}

Thank you for your business!

— The Fotosfolio Team`;

    const html = `
    <div style="font-family:Arial,sans-serif;background-color:#f9f9f9;padding:40px 0;">
      <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:10px;padding:40px;box-shadow:0 0 10px rgba(0,0,0,0.05);">
        
        <!-- Header -->
        <div style="text-align:center;margin-bottom:30px;">
          <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block;margin:0 auto;" />
        </div>

        <!-- Success Icon -->
        <div style="text-align:center;margin-bottom:30px;">
          <div style="width:60px;height:60px;border-radius:50%;background:#4CAF50;display:inline-flex;align-items:center;justify-content:center;">
            <span style="color:#fff;font-size:36px;line-height:1;">✓</span>
          </div>
        </div>

        <!-- Title -->
        <h1 style="color:#333;font-size:24px;text-align:center;margin:0 0 20px;">Payment Successful!</h1>

        <!-- Message -->
        <p style="color:#666;font-size:16px;line-height:1.6;margin:0 0 30px;text-align:center;">
          Hi <strong>${userName}</strong>, your payment has been processed successfully.
        </p>

        <!-- Payment Details -->
        <div style="background:#f8f8f8;border-radius:8px;padding:20px;margin-bottom:30px;">
          <table width="100%" cellpadding="8" cellspacing="0">
            <tr>
              <td style="color:#666;font-size:14px;">Amount:</td>
              <td style="color:#333;font-size:14px;font-weight:bold;text-align:right;">$${amount}</td>
            </tr>
            <tr>
              <td style="color:#666;font-size:14px;">Transaction ID:</td>
              <td style="color:#333;font-size:14px;text-align:right;">${transactionId}</td>
            </tr>
            <tr>
              <td style="color:#666;font-size:14px;">Plan:</td>
              <td style="color:#333;font-size:14px;text-align:right;">${planName}</td>
            </tr>
          </table>
        </div>

        <!-- CTA Button -->
        <div style="text-align:center;margin-bottom:30px;">
          <a href="https://fotosfolio.com/dashboard" style="display:inline-block;padding:12px 30px;background:#4CAF50;color:#fff;text-decoration:none;border-radius:5px;font-size:16px;">View Dashboard</a>
        </div>

        <!-- Footer -->
        <p style="color:#999;font-size:12px;text-align:center;margin:30px 0 0;">
          Thank you for your business!<br>
          — The Fotosfolio Team
        </p>

      </div>
    </div>`;

    return {
      to,
      subject,
      html,
      text,
      category: EmailCategory.PAYMENT,
      type: EmailType.PAYMENT_SUCCESS,
    };
  }

  /**
   * Build PAYMENT_FAILED email
   */
  async buildPaymentFailed(payload: any): Promise<EmailData> {
    const { to, userName, amount, reason, planName } = payload;
    const subject = 'Payment Failed – Action Required';

    const text = `Hi ${userName},

We were unable to process your payment of $${amount} for ${planName}.

Reason: ${reason}

Please update your payment method or try again.

Update Payment Method: https://fotosfolio.com/billing

If you need assistance, please contact our support team.

— The Fotosfolio Team`;

    const html = `
    <div style="font-family:Arial,sans-serif;background-color:#f9f9f9;padding:40px 0;">
      <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:10px;padding:40px;box-shadow:0 0 10px rgba(0,0,0,0.05);">
        
        <!-- Header -->
        <div style="text-align:center;margin-bottom:30px;">
          <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block;margin:0 auto;" />
        </div>

        <!-- Error Icon -->
        <div style="text-align:center;margin-bottom:30px;">
          <div style="width:60px;height:60px;border-radius:50%;background:#f44336;display:inline-flex;align-items:center;justify-content:center;">
            <span style="color:#fff;font-size:36px;line-height:1;">✕</span>
          </div>
        </div>

        <!-- Title -->
        <h1 style="color:#333;font-size:24px;text-align:center;margin:0 0 20px;">Payment Failed</h1>

        <!-- Message -->
        <p style="color:#666;font-size:16px;line-height:1.6;margin:0 0 30px;text-align:center;">
          Hi <strong>${userName}</strong>, we were unable to process your payment.
        </p>

        <!-- Payment Details -->
        <div style="background:#fff5f5;border:1px solid #ffcdd2;border-radius:8px;padding:20px;margin-bottom:30px;">
          <table width="100%" cellpadding="8" cellspacing="0">
            <tr>
              <td style="color:#666;font-size:14px;">Amount:</td>
              <td style="color:#333;font-size:14px;font-weight:bold;text-align:right;">$${amount}</td>
            </tr>
            <tr>
              <td style="color:#666;font-size:14px;">Plan:</td>
              <td style="color:#333;font-size:14px;text-align:right;">${planName}</td>
            </tr>
            <tr>
              <td style="color:#666;font-size:14px;">Reason:</td>
              <td style="color:#d32f2f;font-size:14px;text-align:right;">${reason}</td>
            </tr>
          </table>
        </div>

        <!-- CTA Button -->
        <div style="text-align:center;margin-bottom:30px;">
          <a href="https://fotosfolio.com/billing" style="display:inline-block;padding:12px 30px;background:#f44336;color:#fff;text-decoration:none;border-radius:5px;font-size:16px;">Update Payment Method</a>
        </div>

        <!-- Footer -->
        <p style="color:#999;font-size:12px;text-align:center;margin:30px 0 0;">
          Need help? Contact our support team.<br>
          — The Fotosfolio Team
        </p>

      </div>
    </div>`;

    return {
      to,
      subject,
      html,
      text,
      category: EmailCategory.PAYMENT,
      type: EmailType.PAYMENT_FAILED,
    };
  }

  /**
   * Build REFUND_PROCESSED email
   */
  async buildRefundProcessed(payload: any): Promise<EmailData> {
    const { to, userName, amount, transactionId, reason, refundDate } = payload;
    const subject = 'Refund Processed Successfully';

    const text = `Hi ${userName},

Your refund of $${amount} has been processed successfully.

Transaction ID: ${transactionId}
Refund Date: ${refundDate}
Reason: ${reason}

The amount will be credited to your original payment method within 5-10 business days.

— The Fotosfolio Team`;

    const html = `
    <div style="font-family:Arial,sans-serif;background-color:#f9f9f9;padding:40px 0;">
      <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:10px;padding:40px;box-shadow:0 0 10px rgba(0,0,0,0.05);">
        
        <!-- Header -->
        <div style="text-align:center;margin-bottom:30px;">
          <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block;margin:0 auto;" />
        </div>

        <!-- Success Icon -->
        <div style="text-align:center;margin-bottom:30px;">
          <div style="width:60px;height:60px;border-radius:50%;background:#2196F3;display:inline-flex;align-items:center;justify-content:center;">
            <span style="color:#fff;font-size:36px;line-height:1;">↩</span>
          </div>
        </div>

        <!-- Title -->
        <h1 style="color:#333;font-size:24px;text-align:center;margin:0 0 20px;">Refund Processed</h1>

        <!-- Message -->
        <p style="color:#666;font-size:16px;line-height:1.6;margin:0 0 30px;text-align:center;">
          Hi <strong>${userName}</strong>, your refund has been processed successfully.
        </p>

        <!-- Refund Details -->
        <div style="background:#f8f8f8;border-radius:8px;padding:20px;margin-bottom:30px;">
          <table width="100%" cellpadding="8" cellspacing="0">
            <tr>
              <td style="color:#666;font-size:14px;">Refund Amount:</td>
              <td style="color:#333;font-size:14px;font-weight:bold;text-align:right;">$${amount}</td>
            </tr>
            <tr>
              <td style="color:#666;font-size:14px;">Transaction ID:</td>
              <td style="color:#333;font-size:14px;text-align:right;">${transactionId}</td>
            </tr>
            <tr>
              <td style="color:#666;font-size:14px;">Refund Date:</td>
              <td style="color:#333;font-size:14px;text-align:right;">${refundDate}</td>
            </tr>
            <tr>
              <td style="color:#666;font-size:14px;">Reason:</td>
              <td style="color:#333;font-size:14px;text-align:right;">${reason}</td>
            </tr>
          </table>
        </div>

        <!-- Info Box -->
        <div style="background:#e3f2fd;border-left:4px solid #2196F3;padding:15px;margin-bottom:30px;">
          <p style="color:#1976D2;font-size:14px;margin:0;line-height:1.6;">
            <strong>📌 Note:</strong> The refund will be credited to your original payment method within 5-10 business days.
          </p>
        </div>

        <!-- Footer -->
        <p style="color:#999;font-size:12px;text-align:center;margin:30px 0 0;">
          Thank you for your patience.<br>
          — The Fotosfolio Team
        </p>

      </div>
    </div>`;

    return {
      to,
      subject,
      html,
      text,
      category: EmailCategory.PAYMENT,
      type: EmailType.REFUND_PROCESSED,
    };
  }

  /**
   * Build PAYMENT_SUCCESS email (alternative method name)
   */
  async buildPaymentSuccessfulEmail(
    to: string,
    userName: string,
    planName: string,
  ): Promise<EmailData> {
    const subject = 'Payment Received for Your Subscription';
    const html = `
    <div style="font-family:'Segoe UI',Roboto,Helvetica,Arial,sans-serif;background-color:#f3f3f3;padding:40px 0;">
      <div style="max-width:600px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;box-shadow:0 4px 14px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="145" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding-left:0; padding-right:12px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank" style="text-decoration:none;">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block;" />
                    </a>
                  </td>
                  <td style="padding-left:0; padding-right:12px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank" style="text-decoration:none;">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block;" />
                    </a>
                  </td>
                  <td style="padding-left:0; padding-right:12px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank" style="text-decoration:none;">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block;" />
                    </a>
                  </td>
                  <td style="padding-left:0;">
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank" style="text-decoration:none;">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Title -->
        <div style="text-align:center;padding:16px 25px 6px 25px;">
          <div style="font-size:36px;color:#7b0f0f;margin-bottom:10px;">💳</div>
          <h2 style="color:#7b0f0f;font-size:22px;font-weight:600;margin:0;">Payment Received</h2>
          <p style="margin-top:6px;color:#555;font-size:14px;">Your payment has been successfully received</p>
        </div>

        <!-- Body -->
        <div style="padding:0 40px 20px 40px;text-align:left;">
          <p style="font-size:15px;color:#333;margin:0 0 10px 0;">Hi ${userName},</p>
          <p style="font-size:14px;color:#555;margin:0 0 18px 0;">
            We've successfully received your payment for the <strong style="color:#7b0f0f;">${planName}</strong> plan.
          </p>
          <p style="font-size:14px;color:#555;margin-bottom:25px;">
            Your account activation is currently being processed and will be completed shortly.
          </p>

          <!-- Payment Details Box -->
          <div style="
            border:1px solid #e2e2e2;
            border-radius:10px;
            padding:18px 25px;
            background:#fafafa;
            width:100%;
            margin:0 auto;
          ">
            <h4 style="margin:0 0 12px 0;font-size:15px;font-weight:600;color:#333;">
              Payment Summary
            </h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr style="border-bottom:1px solid #f0f0f0;">
                <td style="padding:8px 0;color:#777;">Plan:</td>
                <td style="text-align:right;font-weight:500;">${planName}</td>
              </tr>
              <tr>
                <td style="padding:8px 0;color:#777;">Status:</td>
                <td style="text-align:right;font-weight:500;color:#7b0f0f;">Payment Received</td>
              </tr>
            </table>
          </div>

          <!-- Button -->
          <div style="text-align:center;margin:35px 0 25px 0;">
            <a href="https://fotosfolio.com/account" style="
              display:inline-block;
              background-color:#7b0f0f;
              color:#fff;
              text-decoration:none;
              padding:12px 28px;
              border-radius:25px;
              font-size:15px;
              font-weight:500;
              box-shadow:0 2px 8px rgba(123,15,15,0.15);
            ">
              Go to My Account
            </a>
          </div>

          <!-- Help Box -->
          <div style="
            background-color:#fafafa;
            border:1px solid #e2e2e2;
            border-radius:10px;
            text-align:center;
            padding:18px 25px;
            font-size:13px;
            color:#555;
          ">
            <strong>Need Assistance?</strong><br />
            You can track your activation status or contact us for any queries.<br />
            For help, <a href="mailto:support@fotosfolio.com" style="color:#7b0f0f;text-decoration:none;">email us</a> or visit our
            <a href="https://fotosfolio.com/help" style="color:#7b0f0f;text-decoration:none;">Help Center</a>.
          </div>

          <!-- Quote -->
          <p style="text-align:center;margin:30px 0 10px 0;font-size:14px;color:#7b0f0f;font-style:italic;">
            "Empowering you to manage your files with confidence and simplicity."
          </p>

          <!-- Signature -->
          <p style="text-align:center;color:#333;font-size:14px;font-weight:600;margin-bottom:10px;">
            — The Fotosfolio Team
          </p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eee;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
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

    const text = `
  Hi ${userName},

  We've received your payment for the ${planName} plan.
  Your account will be activated shortly.

  – The Fotosfolio Team
    `;
    return { to, subject, text, html };
  }

  /**
   * Build PAYMENT_REJECTION email
   */
  async buildPaymentRejectionEmail(
    to: string,
    userName: string,
    planName: string,
  ): Promise<EmailData> {
    const subject = 'Your Payment Could Not Be Completed';

    const text = `Hi ${userName},

Unfortunately, we were unable to process your payment for the ${planName} plan.

This may have happened due to a temporary technical issue or invalid payment details.
Please try again after a few minutes or update your payment information.

If you need help, our support team is ready to assist you.

— The Fotosfolio Team`;

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
          <div style="font-size:34px;color:#7a1217;margin-bottom:8px;">⚠️</div>
          <h2 style="color:#7a1217;font-size:22px;font-weight:600;margin:0;">Payment Unsuccessful</h2>
          <p style="margin-top:6px;color:#555;font-size:14px;">We couldn't complete your payment for the ${planName} plan.</p>
        </div>

        <!-- Body -->
        <div style="padding:0 40px 30px 40px;text-align:left;box-sizing:border-box;">
          <p style="font-size:15px;color:#333;margin:0 0 10px 0;">Hi ${userName},</p>
          <p style="font-size:14px;color:#555;margin:0 0 18px 0;">
            We were unable to process your payment for the <strong style="color:#7a1217;">${planName}</strong> plan.
          </p>
          <p style="font-size:14px;color:#555;margin-bottom:25px;">
            This may have occurred due to temporary technical issues or incorrect payment information.
            Please try again later or update your payment details to continue your subscription.
          </p>

          <!-- Error Box -->
          <div style="border:1px solid #f5caca;border-radius:10px;background:#fdeeee;width:100%;margin:0 auto 25px;box-sizing:border-box;">
            <div style="padding:16px 20px;box-sizing:border-box;">
              <h4 style="margin:0 0 10px 0;font-size:15px;font-weight:600;color:#b32020;">Payment Details</h4>
              <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;box-sizing:border-box;">
                <tr style="border-bottom:1px solid #f8dcdc;">
                  <td style="padding:8px 0;color:#777;">Plan:</td>
                  <td style="text-align:right;font-weight:500;">${planName}</td>
                </tr>
                <tr>
                  <td style="padding:8px 0;color:#777;">Status:</td>
                  <td style="text-align:right;font-weight:600;color:#b32020;">Payment Failed</td>
                </tr>
              </table>
            </div>
          </div>

          <!-- Retry Button -->
          <div style="text-align:center;margin:30px 0 25px 0;">
            <a href="https://fotosfolio.com/payment" style="
              display:inline-block;
              background-color:#7a1217;
              color:#fff;
              text-decoration:none;
              padding:12px 28px;
              border-radius:25px;
              font-size:15px;
              font-weight:500;">
              Retry Payment
            </a>
          </div>

          <!-- Help Box -->
          <div style="background-color:#faf9f9;border:1px solid #f2f2f2;border-radius:10px;text-align:center;padding:20px 25px;font-size:13px;color:#555;margin-bottom:25px;box-sizing:border-box;">
            <strong>Need Assistance?</strong><br />
            Our support team is available to help you resolve payment issues.<br />
            <a href="mailto:support@fotosfolio.com" style="color:#7a1217;text-decoration:none;">Contact us</a> or visit our
            <a href="https://fotosfolio.com/help" style="color:#7a1217;text-decoration:none;">Help Center</a>.
          </div>

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
}
