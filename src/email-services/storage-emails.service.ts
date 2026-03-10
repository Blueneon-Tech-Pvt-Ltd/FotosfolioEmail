import { Injectable } from '@nestjs/common';
import { BaseEmailService } from './base-email.service';
import { EmailData, EmailCategory, EmailType } from '../types/email-types';
import { AddonExpiryEmailDto } from 'src/cron-jobs/dto/addonexpiry.dto';

@Injectable()
export class StorageEmailsService extends BaseEmailService {
  
  /**
   * Build STORAGE_WARNING email (80% full)
   */
  async buildStorageWarning(payload: any): Promise<EmailData> {
    const { to, userName, storageUsed, storageLimit, percentUsed } = payload;
    const subject = 'Storage Warning: Your Storage is Almost Full';
    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140"
                style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding:0 6px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Body -->
        <div style="width:100%;background-color:#fff;padding:0 0 40px 0;line-height:1.6;">
          <div style="text-align:center;padding:40px 25px 15px 25px;border-bottom:1px solid #f0f0f0;">
            <h2 style="margin:0;font-size:22px;font-weight:600;color:#ff9800;">Storage Alert</h2>
            <p style="margin-top:10px;font-size:15px;color:#555;">
              Your storage is running low.
            </p>
          </div>

          <p style="text-align:center;font-size:13px;color:#777;margin:20px 25px 0 25px;">
            Hi <strong>${userName}</strong>, your storage space is reaching its limit.
          </p>

          <div style="margin:30px auto;background:#fff;border:1px solid #e6e6e6;border-radius:10px;
            padding:22px 28px;box-shadow:0 2px 8px rgba(0,0,0,0.04);width:calc(100% - 150px);">
            <h4 style="margin:0 0 14px 0;font-size:15px;font-weight:600;color:#333;text-align:left;">
              Storage Summary
            </h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr style="border-bottom:1px solid #f3f3f3;">
                <td style="padding:8px 0;color:#777;">Storage Used</td>
                <td style="text-align:right;font-weight:500;">${storageUsed} GB</td>
              </tr>
              <tr style="border-bottom:1px solid #f3f3f3;">
                <td style="padding:8px 0;color:#777;">Storage Limit</td>
                <td style="text-align:right;font-weight:500;">${storageLimit} GB</td>
              </tr>
              <tr>
                <td style="padding:8px 0;color:#777;">Usage</td>
                <td style="text-align:right;font-weight:600;color:#ff9800;">${percentUsed}% Full</td>
              </tr>
            </table>
          </div>

          <p style="text-align:center;font-size:14px;color:#ff9800;margin:25px 35px 0 35px;font-weight:600;">
            Your storage is ${percentUsed}% full.
          </p>
          <p style="text-align:center;font-size:13px;color:#555;margin:10px 35px 0 35px;">
            Consider upgrading your plan or deleting unused files to avoid service interruption.
          </p>

          <div style="text-align:center;margin:28px 0 38px 0;">
            <a href="https://fotosfolio.com/upgrade" style="display:inline-block;background-color:#8B1E1E;color:#ffffff;
              text-decoration:none;padding:13px 32px;border-radius:25px;font-weight:500;font-size:15px;">
              Upgrade Plan
            </a>
          </div>

          <div style="height:1px;background-color:#eeeeee;margin:20px 60px;"></div>

          <p style="text-align:center;font-size:13px;color:#777;margin:0 40px 20px 40px;">
            Need help managing your storage? Our support team is always ready to assist.
          </p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717;text-decoration:none;margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717;text-decoration:none;margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717;text-decoration:none;margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>`;
    const text = `Hi ${userName},\n\nYour storage is ${percentUsed}% full (${storageUsed} GB of ${storageLimit} GB).\n\nConsider upgrading your plan or deleting unused files.`;
    return {
      to,
      subject,
      html,
      text,
      category: EmailCategory.STORAGE,
      type: EmailType.STORAGE_WARNING,
    };
  }
  
  /**
   * Build STORAGE_FULL email (100% full)
   */
  async buildStorageFull(payload: any): Promise<EmailData> {
    const { to, userName, storageLimit } = payload;
    
    const subject = 'Storage Full: Immediate Action Required';
    
    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140"
                style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding:0 6px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Body -->
        <div style="width:100%;background-color:#fff;padding:0 0 40px 0;line-height:1.6;">
          <div style="text-align:center;padding:40px 25px 15px 25px;border-bottom:1px solid #f0f0f0;">
            <h2 style="margin:0;font-size:22px;font-weight:600;color:#8B1E1E;">Storage Full</h2>
            <p style="margin-top:10px;font-size:15px;color:#555;">
              Immediate action required.
            </p>
          </div>

          <p style="text-align:center;font-size:13px;color:#777;margin:20px 25px 0 25px;">
            Hi <strong>${userName}</strong>, your storage capacity has been reached.
          </p>

          <div style="margin:30px auto;background:#fff;border:1px solid #e6e6e6;border-radius:10px;
            padding:22px 28px;box-shadow:0 2px 8px rgba(0,0,0,0.04);width:calc(100% - 150px);">
            <h4 style="margin:0 0 14px 0;font-size:15px;font-weight:600;color:#333;text-align:left;">
              Storage Status
            </h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr style="border-bottom:1px solid #f3f3f3;">
                <td style="padding:8px 0;color:#777;">Storage Limit</td>
                <td style="text-align:right;font-weight:500;">${storageLimit} GB</td>
              </tr>
              <tr>
                <td style="padding:8px 0;color:#777;">Status</td>
                <td style="text-align:right;font-weight:600;color:#8B1E1E;">100% Full</td>
              </tr>
            </table>
          </div>

          <p style="text-align:center;font-size:14px;color:#8B1E1E;margin:25px 35px 0 35px;font-weight:600;">
            Your storage is completely full.
          </p>
          <p style="text-align:center;font-size:13px;color:#555;margin:10px 35px 0 35px;">
            You cannot upload new files until you free up space or upgrade your plan.
          </p>

          <div style="text-align:center;margin:28px 0 38px 0;">
            <a href="https://fotosfolio.com/upgrade" style="display:inline-block;background-color:#8B1E1E;color:#ffffff;
              text-decoration:none;padding:13px 32px;border-radius:25px;font-weight:500;font-size:15px;margin-right:10px;">
              Upgrade Plan
            </a>
            <a href="https://fotosfolio.com/files" style="display:inline-block;background-color:#d9534f;color:#ffffff;
              text-decoration:none;padding:13px 32px;border-radius:25px;font-weight:500;font-size:15px;">
              Delete Files
            </a>
          </div>

          <div style="height:1px;background-color:#eeeeee;margin:20px 60px;"></div>

          <p style="text-align:center;font-size:13px;color:#777;margin:0 40px 20px 40px;">
            Need help managing your storage? Our support team is ready to assist you.
          </p>
        </div>

        <!-- Footer -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
          <p style="margin:0;line-height:1.6;">
            © ${new Date().getFullYear()} Fotosfolio. All rights reserved.<br>
            <a href="https://fotosfolio.com/privacy" style="color:#7b1717;text-decoration:none;margin:0 6px;">Privacy Policy</a> •
            <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717;text-decoration:none;margin:0 6px;">Terms of Service</a> •
            <a href="https://fotosfolio.com/contact-us" style="color:#7b1717;text-decoration:none;margin:0 6px;">Contact Support</a>
          </p>
        </div>

      </div>
    </div>`;
    
    const text = `Hi ${userName},\n\nYour storage is completely full (${storageLimit} GB). Upgrade your plan or delete files to continue uploading.`;
    
    return {
      to,
      subject,
      html,
      text,
      category: EmailCategory.STORAGE,
      type: EmailType.STORAGE_FULL,
    };
  }
  
  /**
   * Build ADDON_EXPIRY email (legacy)
   */
  async buildAddonExpiry(payload: any): Promise<EmailData> {
    const contact: AddonExpiryEmailDto = payload;
    
    // Validate required fields
    if (!contact.userEmail) {
      throw new Error('Missing required field: userEmail');
    }
    if (contact.addonLimit === undefined || contact.addonLimit === null) {
      throw new Error('Missing required field: addonLimit');
    }
    if (contact.newStorageLimit === undefined || contact.newStorageLimit === null) {
      throw new Error('Missing required field: newStorageLimit');
    }
    if (contact.storageUsed === undefined || contact.storageUsed === null) {
      throw new Error('Missing required field: storageUsed');
    }
    if (!contact.renewLink) {
      throw new Error('Missing required field: renewLink');
    }
    
    const subject = `Your Addon Storage of ${contact.addonLimit} GB Has Expired`;
    const exceedsLimit =
      Number(contact.storageUsed) > Number(contact.newStorageLimit);

    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header (same as sendProjectInvitation) -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140"
                style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding:0 6px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                  <td style="padding:0 6px;">
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp"
                        style="display:block; border:none; outline:none; text-decoration:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- BODY (kept exactly same as before) -->
        <div style="width:100%;background-color:#fff;padding:0 0 40px 0;line-height:1.6;">
          <div style="text-align:center;padding:40px 25px 15px 25px;border-bottom:1px solid #f0f0f0;">
            <h2 style="margin:0;font-size:22px;font-weight:600;color:#8B1E1E;">Addon Storage Expired</h2>
            <p style="margin-top:10px;font-size:15px;color:#555;">
              Your additional storage plan has ended.
            </p>
          </div>

          <p style="text-align:center;font-size:13px;color:#777;margin:20px 25px 0 25px;">
            Here’s a summary of the recent update to your Fotosfolio account.
          </p>

          <div style="margin:30px auto;background:#fff;border:1px solid #e6e6e6;border-radius:10px;
            padding:22px 28px;box-shadow:0 2px 8px rgba(0,0,0,0.04);width:calc(100% - 150px);">
            <h4 style="margin:0 0 14px 0;font-size:15px;font-weight:600;color:#333;text-align:left;">
              Storage Summary
            </h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr style="border-bottom:1px solid #f3f3f3;">
                <td style="padding:8px 0;color:#777;">Previous Addon Limit</td>
                <td style="text-align:right;font-weight:500;">${contact.addonLimit} GB</td>
              </tr>
              <tr style="border-bottom:1px solid #f3f3f3;">
                <td style="padding:8px 0;color:#777;">Current Storage Limit</td>
                <td style="text-align:right;font-weight:500;">${contact.newStorageLimit} GB</td>
              </tr>
              <tr style="border-bottom:1px solid #f3f3f3;">
                <td style="padding:8px 0;color:#777;">Current Usage</td>
                <td style="text-align:right;font-weight:500;">${contact.storageUsed} GB</td>
              </tr>
              ${
                contact.graceApplied
                  ? `
              <tr>
                <td style="padding:8px 0;color:#777;">Grace Period Ends</td>
                <td style="text-align:right;font-weight:600;color:#8B1E1E;">
                  ${contact.graceEnd?.toLocaleString()}
                </td>
              </tr>`
                  : ''
              }
            </table>
          </div>

          ${
            contact.graceApplied && exceedsLimit
              ? `
              <p style="text-align:center; font-size:14px; color:#8B1E1E; margin:25px 35px 0 35px; line-height:1.6;">
                Your additional storage Plan has expired, and your current usage exceeds your plan's limit.
                </br>
                A temporary grace period has been applied until
                <strong>${contact.graceEnd?.toLocaleString()}</strong>.
              </p>
              <p style="text-align:center; font-size:14px; color:#8B1E1E; margin:15px 35px 0 35px; line-height:1.6;">
                To avoid any disruption, please consider renewing your plan or managing your storage accordingly.
              </p>

          <p style="text-align:center;font-size:13px;color:#555;margin:10px 35px 0 35px;">
            Please renew your addon plan or reduce your storage usage before the grace period ends to avoid upload interruptions.
          </p>
          `
              : `
          <p style="text-align:center;font-size:14px;color:#333;margin:25px 35px 0 35px;">
            Your addon storage plan has expired, and your account has been reverted to the standard storage limit.
          </p>
          <p style="text-align:center;font-size:13px;color:#555;margin:10px 35px 0 35px;">
            To continue enjoying extended storage and uninterrupted service, you can renew your addon plan anytime.
          </p>
          `
          }

          <div style="text-align:center;margin:28px 0 38px 0;">
            <a href="${contact.renewLink}" style="display:inline-block;background-color:#8B1E1E;color:#ffffff;
              text-decoration:none;padding:13px 32px;border-radius:25px;font-weight:500;font-size:15px;">
              Renew Storage Plan
            </a>
          </div>

          <div style="height:1px;background-color:#eeeeee;margin:20px 60px;"></div>

          <p style="text-align:center;font-size:13px;color:#777;margin:0 40px 20px 40px;">
            Need help managing your plan or storage? Our support team is always ready to assist.
          </p>
        </div>

        <!-- Footer (same as sendProjectInvitation) -->
        <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
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
    return {
      to: contact.userEmail,
      subject,
      html,
      text: `Your addon storage of ${contact.addonLimit} GB has expired.`,
      category: EmailCategory.STORAGE,
      type: EmailType.ADDON_EXPIRY,
    };
  }

  /**
   * Build ADDON_FINAL_GRACE email (legacy)
   */
  buildAddonFinalGrace(payload: any): EmailData {
    const contact = payload;
    
    // Validate required fields
    if (!contact.userEmail) {
      throw new Error('Missing required field: userEmail');
    }
    if (contact.graceDaysRemaining === undefined || contact.graceDaysRemaining === null) {
      throw new Error('Missing required field: graceDaysRemaining');
    }
    if (contact.storageUsed === undefined || contact.storageUsed === null) {
      throw new Error('Missing required field: storageUsed');
    }
    if (!contact.renewLink) {
      throw new Error('Missing required field: renewLink');
    }
    if (!contact.deleteLink) {
      throw new Error('Missing required field: deleteLink');
    }
    
   const subject = `Final Reminder: ${contact.graceDaysRemaining} Day${contact.graceDaysRemaining > 1 ? 's' : ''} Remaining Before Data Deletion`;

    const html = `
    <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
      <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

        <!-- Header -->
        <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
          <tr>
            <td align="left" valign="middle" style="padding-bottom:18px;">
              <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140"
                style="display:block;outline:none;text-decoration:none;border:none;-ms-interpolation-mode:bicubic;" />
            </td>
            <td align="right" valign="middle" style="padding-bottom:18px;">
              <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                <tr>
                  <td style="padding-right:12px;">
                    <a href="https://www.instagram.com/fotosfolio.np/" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram"
                        style="display:block;border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://linkedin.com/company/fotosfolio" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn"
                        style="display:block;border:none;" />
                    </a>
                  </td>
                  <td style="padding-right:12px;">
                    <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook"
                        style="display:block;border:none;" />
                    </a>
                  </td>
                  <td>
                    <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank">
                      <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp"
                        style="display:block;border:none;" />
                    </a>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
        </table>

        <!-- Email Body -->
        <div style="padding:30px;text-align:center;">

          <h2 style="font-size:20px;color:#8b0d0d;margin-bottom:12px;">Final Grace Period Reminder</h2>
          <p style="color:#555;font-size:14px;margin-bottom:20px;">
            Your account requires immediate attention to avoid potential data loss.
          </p>

          <p style="font-size:13px;color:#666;margin:25px 25px 0 25px;">
            Your addon storage plan has expired, and your current usage exceeds the storage available on your account.
          </p>

          <!-- Account Summary -->
          <div style="margin:30px auto;background:#fff;border:1px solid #ebebeb;border-radius:10px;padding:22px 28px;
              box-shadow:0 2px 8px rgba(0,0,0,0.03);width:calc(100% - 150px);text-align:left;">
            <h4 style="margin:0 0 14px 0;font-size:15px;font-weight:600;color:#333;">Account Summary</h4>
            <table style="width:100%;border-collapse:collapse;font-size:14px;color:#333;">
              <tr style="border-bottom:1px solid #f3f3f3;">
                <td style="padding:8px 0;color:#777;text-align:left;">Current Usage</td>
                <td style="padding:8px 0;text-align:right;font-weight:500;">${contact.storageUsed} GB</td>
              </tr>
              <tr>
                <td style="padding:8px 0;color:#777;text-align:left;">Days Remaining</td>
                <td style="padding:8px 0;text-align:right;font-weight:600;color:#8b0d0d;">
                  ${contact.graceDaysRemaining} Day${contact.graceDaysRemaining > 1 ? 's' : ''}
                </td>
              </tr>
            </table>
          </div>

          <p style="font-size:14px;color:#8b0d0d;margin:25px 35px 0 35px;font-weight:600;">
            Your grace period will end in ${contact.graceDaysRemaining} day${contact.graceDaysRemaining > 1 ? 's' : ''}.
          </p>
          <p style="font-size:13px;color:#555;margin:10px 35px 0 35px;">
            To maintain uninterrupted access to your files, please renew your plan or reduce your storage usage before the grace period expires.
          </p>
          <p style="font-size:13px;color:#8b0d0d;margin:10px 35px 0 35px;font-weight:500;">
            If your usage remains above the allowed limit after the grace period ends, excess data will be permanently deleted.
            This process is automated, and we cannot guarantee which specific files will be removed. Deleted files cannot be restored.
          </p>

          <!-- Buttons -->
          <div style="margin:32px 0 40px 0;">
            <a href="${contact.renewLink}" style="
              display:inline-block;background-color:#8b0d0d;color:#ffffff;text-decoration:none;
              padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;margin-right:10px;">
              Renew Plan
            </a>
            <a href="${contact.deleteLink}" style="
              display:inline-block;background-color:#d9534f;color:#ffffff;text-decoration:none;
              padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;">
              Delete Files
            </a>
          </div>

          <!-- Divider -->
          <div style="height:1px;background-color:#eeeeee;margin:20px 60px;"></div>

          <p style="font-size:13px;color:#8b0d0d;margin:0 40px 10px 40px;font-weight:500;">
            Once deleted, your files cannot be recovered. Please act before your grace period expires.
          </p>
          <p style="font-size:13px;color:#777;margin:0 40px 25px 40px;">
            If you need help renewing your plan or managing your storage, our support team is available to assist you.
          </p>
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
    return {
      to: contact.userEmail,
      subject,
      html,
      text: `Final reminder: ${contact.graceDaysRemaining} days remaining before data deletion.`,
      category: EmailCategory.STORAGE,
      type: EmailType.ADDON_FINAL_GRACE,
    };
  }
}