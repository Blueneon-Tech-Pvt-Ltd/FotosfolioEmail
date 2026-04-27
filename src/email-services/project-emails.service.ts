// src/email-services/project-emails.service.ts
import { Injectable } from '@nestjs/common';
import { BaseEmailService, EmailData } from './base-email.service';

@Injectable()
export class ProjectEmailsService extends BaseEmailService {

  async buildProjectTransferEmail(
    receiverEmail: string,
    receiverName: string,
    senderName: string,
    projectName: string,
    projectLink: string,
    totalFiles: number,
    totalSize: string
  ): Promise<EmailData> {
    console.log(`Sending project transfer email to ${receiverEmail}`);
    const subject = 'Project Transfer Request – Action Required';

    const text = `Dear ${receiverName},

${senderName} has requested to transfer the project "${projectName}" to your account.
This project contains ${totalFiles} files with a total size of ${totalSize} GB.

To review and accept this project transfer, please click the link below:

Accept Project Transfer: ${projectLink}

If you did not expect this request or believe it was sent in error, you may safely ignore this email. The project will not be transferred unless you accept the request.

If you have any questions, feel free to contact us.

Kind regards,
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
          <h2 style="color:#7a1217; font-size:18px; margin-bottom:20px;">Project Transfer Request</h2>

          <p style="font-size:13px; color:#666; margin:0 0 25px;">
            Dear <strong>${receiverName}</strong>,
          </p>

          <p style="font-size:14px; color:#333; margin-bottom:5px; font-weight:600;">
            <strong>${senderName}</strong> has requested to transfer the project <strong>"${projectName}"</strong> to your account.
          </p>
          <p style="font-size:13px; color:#333; margin-bottom:30px;">
            This project contains ${totalFiles} files with a total size of ${totalSize} GB.
          </p>

          <p style="font-size:12px; color:#7a1217; margin-bottom:15px;">
            To review and accept this project transfer, please click below:
          </p>

          <!-- Project Details Card -->
          <div style="background-color:#f8f8f8; border-radius:8px; padding:20px; margin:30px 0; text-align:left;">
            <h3 style="color:#7a1217; font-size:14px; margin:0 0 15px; text-align:center; font-weight:600;">Project Details</h3>

            <table width="100%" cellpadding="8" cellspacing="0" style="font-size:12px;">
              <tr>
                <td style="color:#666; padding:8px 0;"><strong>Project Name:</strong></td>
                <td style="color:#333; padding:8px 0; text-align:right;">${projectName}</td>
              </tr>
              <tr style="border-top:1px solid #e0e0e0;">
                <td style="color:#666; padding:8px 0;"><strong>Total Files:</strong></td>
                <td style="color:#333; padding:8px 0; text-align:right;">${totalFiles} files</td>
              </tr>
              <tr style="border-top:1px solid #e0e0e0;">
                <td style="color:#666; padding:8px 0;"><strong>Total Size:</strong></td>
                <td style="color:#333; padding:8px 0; text-align:right;">${totalSize} GB</td>
              </tr>
              <tr style="border-top:1px solid #e0e0e0;">
                <td style="color:#666; padding:8px 0;"><strong>Requested by:</strong></td>
                <td style="color:#333; padding:8px 0; text-align:right;">${senderName}</td>
              </tr>
            </table>
          </div>

          <p style="margin:25px 0;">
            <a href="${projectLink}"
               style="background-color:#7a1217; color:#fff; text-decoration:none;
                      padding:10px 25px; border-radius:20px; font-size:13px; display:inline-block;">
              Accept Project Transfer
            </a>
          </p>

          <hr style="border:none; border-top:1px solid #eee; margin:40px 0;">

          <p style="font-size:12px; color:#333; font-weight:600; margin-bottom:10px;">
            Important Information
          </p>
          <p style="font-size:12px; color:#666; margin:0 0 25px;">
            If you did not expect this request or believe it was sent in error, you may safely ignore this email. The project will not be transferred unless you accept the request. If you have any questions, feel free to
            <a href="mailto:support@fotosfolio.com" style="color:#7a1217; text-decoration:none;">contact us</a>.
          </p>

          <p style="font-size:12px; color:#333; margin:30px 0 5px;">Kind regards,</p>
          <p style="font-size:12px; font-weight:600; color:#333; margin:0;">The Fotosfolio Team</p>
        </div>
      </div>

      <div style="max-width:600px; margin:0 auto; text-align:center; padding:20px 0; font-size:11px; color:#999;">
        <p style="margin:5px 0;">© 2025 Fotosfolio. All rights reserved.</p>
        <p style="margin:5px 0;">
          <a href="https://fotosfolio.com/privacy" style="color:#999; text-decoration:none;">Privacy Policy</a> |
          <a href="https://fotosfolio.com/terms-and-conditions" style="color:#999; text-decoration:none;">Terms of Service</a> |
          <a href="https://fotosfolio.com/contact-us" style="color:#999; text-decoration:none;">Contact Support</a> |
          <a href="https://fotosfolio.com/unsubscribe" style="color:#999; text-decoration:none;">Unsubscribe</a>
        </p>
      </div>
    </div>`;
    console.log('Project transfer email sent successfully');
    return { to: receiverEmail, subject, text, html };
  }

  async buildAccessRequestEmail(
    projectName: string,
    requesterEmail: string,
    ownerEmail: string,
    photographerName: string,
    projectId: string
  ): Promise<EmailData> {
    const currentTime = new Date().toLocaleString('en-US', {
      timeZone: 'Asia/Kathmandu',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
    return {
      to: ownerEmail,
      subject: `Access Request for ${projectName} Gallery`,
      text: `${requesterEmail} has requested access to your ${projectName} gallery.`,
      html: `
      <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
        <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

          <!-- Header (logo left, icons right) -->
          <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
            <tr>
              <td align="left" valign="middle" style="padding-bottom:18px;">
                <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
              </td>
              <td align="right" valign="middle" style="padding-bottom:18px;">
                <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                  <tr>
                    <td style="padding:0 6px;">
                      <a href="https://www.instagram.com/fotosfolio.np/" target="_blank" style="text-decoration:none;">
                        <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block; border:none; outline:none; text-decoration:none;" />
                      </a>
                    </td>
                    <td style="padding:0 6px;">
                      <a href="https://linkedin.com/company/fotosfolio" target="_blank" style="text-decoration:none;">
                        <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block; border:none; outline:none; text-decoration:none;" />
                      </a>
                    </td>
                    <td style="padding:0 6px;">
                      <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank" style="text-decoration:none;">
                        <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block; border:none; outline:none; text-decoration:none;" />
                      </a>
                    </td>
                    <td style="padding:0 6px;">
                      <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank" style="text-decoration:none;">
                        <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block; border:none; outline:none; text-decoration:none;" />
                      </a>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>

          <!-- Body (UNCHANGED content & styling preserved) -->
          <div style="padding:50px 50px 40px 50px;text-align:center;">
            <h2 style="color:#8b0000;font-size:18px;font-weight:600;margin:0 0 25px;">
              Access Request for ${projectName} Gallery
            </h2>

            <p style="font-size:15px;color:#333;margin:0 0 6px;">Hi ${photographerName},</p>
            <p style="font-size:14px;color:#333;margin:0 0 25px;line-height:1.5;">
              <a href="mailto:${requesterEmail}" style="color:#b22222;text-decoration:none;">${requesterEmail}</a> has requested access to your gallery.
            </p>

            <!-- Request Details Box -->
            <div style="border-left:4px solid #b22222;background-color:#fff;border-radius:6px;padding:40px 40px;margin:0 auto 40px auto;text-align:left;max-width:470px;">
              <p style="margin:0 0 16px;font-size:14px;font-weight:600;color:#333;">Request Details</p>

              <table style="width:100%;border-collapse:collapse;font-size:13px;">
                <tr>
                  <td style="padding:10px 0;color:#666;">Email:</td>
                  <td style="padding:10px 0;text-align:right;">
                    <a href="mailto:${requesterEmail}" style="color:#b22222;text-decoration:none;">${requesterEmail}</a>
                  </td>
                </tr>
                <tr>
                  <td style="padding:10px 0;color:#666;">Time:</td>
                  <td style="padding:10px 0;color:#111;text-align:right;">${currentTime}</td>
                </tr>
              </table>
            </div>

            <!-- Button -->
            <div style="margin-bottom:35px;">
              <a href="https://fotosfolio.com/dashboard/projects/${projectId}/albums"
                target="_blank"
                style="background-color:#f8d7da;color:#721c24;text-decoration:none;padding:12px 40px;border-radius:25px;font-size:14px;font-weight:600;display:inline-block;">
                Grant Access
              </a>
            </div>

            <p style="font-size:12px;color:#777;margin:0;">
              If you don’t recognize this user, you can ignore this email.
            </p>
          </div>

          <!-- Footer (matches sendProjectInvitation style; no Unsubscribe) -->
          <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
            <p style="margin:0;line-height:1.6;">
              © 2025 Fotosfolio. All rights reserved.<br>
              <a href="https://fotosfolio.com/privacy" style="color:#7b1717;text-decoration:none;margin:0 6px;">Privacy Policy</a> •
              <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717;text-decoration:none;margin:0 6px;">Terms of Service</a> •
              <a href="https://fotosfolio.com/contact-us" style="color:#7b1717;text-decoration:none;margin:0 6px;">Contact Support</a>
            </p>
          </div>

        </div>
      </div>
      `,
    };
  }

  async buildFavouriteAccessRequestEmail(
    projectName: string,
    requesterEmail: string,
    ownerEmail: string,
    photographerName: string,
    projectId: string
  ): Promise<EmailData> {
    const currentTime = new Date().toLocaleString('en-US', {
      timeZone: 'Asia/Kathmandu',
      year: 'numeric',
      month: 'long',
      day: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
    });
    return {
      to: ownerEmail,
      subject: `Access Request for Favorites - ${projectName}`,
      text: `${requesterEmail} has requested access to your favorite list for the project "${projectName}".`,
      html: `
      <div style="background-color:#f4f4f7;padding:40px 0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,'Helvetica Neue',Arial,sans-serif;">
        <div style="max-width:600px;margin:0 auto;background-color:#ffffff;border-radius:10px;overflow:hidden;box-shadow:0 5px 15px rgba(0,0,0,0.05);">

          <!-- Header (logo left, icons right) -->
          <table width="100%" cellpadding="0" cellspacing="0" role="presentation" style="padding:20px 28px 0 28px;">
            <tr>
              <td align="left" valign="middle" style="padding-bottom:18px;">
                <img src="https://cdn.fotosfolio.com/logo3.png" alt="Fotosfolio" width="140" style="display:block; outline:none; text-decoration:none; border:none; -ms-interpolation-mode:bicubic;" />
              </td>
              <td align="right" valign="middle" style="padding-bottom:18px;">
                <table cellpadding="0" cellspacing="0" role="presentation" style="display:inline-block;">
                  <tr>
                    <td style="padding:0 6px;">
                      <a href="https://www.instagram.com/fotosfolio.np/" target="_blank" style="text-decoration:none;">
                        <img src="https://cdn-icons-png.flaticon.com/512/1384/1384063.png" width="20" alt="Instagram" style="display:block; border:none; outline:none; text-decoration:none;" />
                      </a>
                    </td>
                    <td style="padding:0 6px;">
                      <a href="https://linkedin.com/company/fotosfolio" target="_blank" style="text-decoration:none;">
                        <img src="https://cdn-icons-png.flaticon.com/512/1384/1384014.png" width="20" alt="LinkedIn" style="display:block; border:none; outline:none; text-decoration:none;" />
                      </a>
                    </td>
                    <td style="padding:0 6px;">
                      <a href="https://www.facebook.com/profile.php?id=61575539292098" target="_blank" style="text-decoration:none;">
                        <img src="https://cdn-icons-png.flaticon.com/512/1384/1384005.png" width="20" alt="Facebook" style="display:block; border:none; outline:none; text-decoration:none;" />
                      </a>
                    </td>
                    <td style="padding:0 6px;">
                      <a href="https://whatsapp.com/channel/0029VbBIFPb8kyyIa0ILHs2M" target="_blank" style="text-decoration:none;">
                        <img src="https://cdn-icons-png.flaticon.com/512/733/733585.png" width="20" alt="WhatsApp" style="display:block; border:none; outline:none; text-decoration:none;" />
                      </a>
                    </td>
                  </tr>
                </table>
              </td>
            </tr>
          </table>

          <!-- Body -->
          <div style="padding:50px 50px 40px 50px;text-align:center;">
            <h2 style="color:#8b0000;font-size:18px;font-weight:600;margin:0 0 25px;">
              Favorites Access Request
            </h2>

            <p style="font-size:15px;color:#333;margin:0 0 6px;">Hi ${photographerName},</p>
            <p style="font-size:14px;color:#333;margin:0 0 25px;line-height:1.5;">
              <a href="mailto:${requesterEmail}" style="color:#b22222;text-decoration:none;">${requesterEmail}</a> has requested access to favourites from the <strong>${projectName}</strong> gallery.
            </p>

            <!-- Request Details Box -->
            <div style="border-left:4px solid #b22222;background-color:#fff;border-radius:6px;padding:40px 40px;margin:0 auto 40px auto;text-align:left;max-width:470px;">
              <p style="margin:0 0 16px;font-size:14px;font-weight:600;color:#333;">Request Details</p>

              <table style="width:100%;border-collapse:collapse;font-size:13px;">
                <tr>
                  <td style="padding:10px 0;color:#666;">Project:</td>
                  <td style="padding:10px 0;text-align:right;color:#111;font-weight:600;">${projectName}</td>
                </tr>
                <tr>
                  <td style="padding:10px 0;color:#666;">Requested By:</td>
                  <td style="padding:10px 0;text-align:right;">
                    <a href="mailto:${requesterEmail}" style="color:#b22222;text-decoration:none;">${requesterEmail}</a>
                  </td>
                </tr>
                <tr>
                  <td style="padding:10px 0;color:#666;">Time:</td>
                  <td style="padding:10px 0;color:#111;text-align:right;">${currentTime}</td>
                </tr>
              </table>
            </div>

            <!-- Button -->
            <div style="margin-bottom:35px;">
              <a href="https://fotosfolio.com/dashboard/projects/${projectId}/favorites"
                target="_blank"
                style="background-color:#f8d7da;color:#721c24;text-decoration:none;padding:12px 40px;border-radius:25px;font-size:14px;font-weight:600;display:inline-block;">
                Review Request
              </a>
            </div>

            <p style="font-size:12px;color:#777;margin:0;">
              If you don’t recognize this request, you can safely ignore this email.
            </p>
          </div>

          <!-- Footer -->
          <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
            <p style="margin:0;line-height:1.6;">
              © 2025 Fotosfolio. All rights reserved.<br>
              <a href="https://fotosfolio.com/privacy" style="color:#7b1717;text-decoration:none;margin:0 6px;">Privacy Policy</a> •
              <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717;text-decoration:none;margin:0 6px;">Terms of Service</a> •
              <a href="https://fotosfolio.com/contact-us" style="color:#7b1717;text-decoration:none;margin:0 6px;">Contact Support</a>
            </p>
          </div>

        </div>
      </div>
      `,
    };
  }

  async buildProjectInvitationEmail(
    projectName: string,
    photographerName: string,
    receiverEmail: string,
    invitationLink: string,
  ): Promise<EmailData> {
    return {
      to: receiverEmail,
      subject: `Your Gallery Invitation for "${projectName}" — FotosFolio`,
      text: `${photographerName} has invited you to view their ${projectName} gallery on FotosFolio.`,
      html: `
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
            <h2 style="font-size:20px;color:#111;margin-bottom:12px;">You're Invited to View Your Gallery</h2>

            <p style="color:#555;font-size:15px;margin-bottom:20px;">
              Hello, </br>
              ${receiverEmail}

            </p>

            <p style="color:#555;font-size:15px;line-height:1.6;margin-bottom:25px;">
              <strong>${photographerName}</strong> has prepared a gallery titled
              <strong>“${projectName}”</strong>  for you.</br>
              Explore your gallery and enjoy the beautifully curated collection.

            <!-- Highlight Box -->
            <div style="background-color:#f8eaea;border-left:3px solid #8b0d0d;border-radius:8px;padding:16px 18px;margin:0 auto 24px;max-width:90%;">
              <p style="color:#444;font-size:14px;margin:0;">
                “Click the button below to access your gallery and view your project.”
              </p>
            </div>

            <!-- Divider -->
            <div style="margin:20px 0;">
              <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
              <span style="display:inline-block;width:6px;height:6px;background:#8b0d0d;border-radius:50%;margin:0 6px;vertical-align:middle;"></span>
              <span style="display:inline-block;width:50px;height:1px;background:#ccc;vertical-align:middle;"></span>
            </div>

            <!-- Call-to-Action Button -->
            <a href="${invitationLink}" target="_blank"
               style="display:inline-block;background-color:#8b0d0d;color:#fff;text-decoration:none;
               padding:12px 30px;border-radius:25px;font-weight:600;font-size:14px;margin-bottom:35px;">
              View Your Gallery
            </a>

            <!-- Assistance Box -->
            <div style="background-color:#fafafa;border:1px solid #e2e2e2;border-radius:8px;padding:18px;margin-top:15px;">
              <p style="font-size:14px;color:#555;margin:0;">
                If you experience any issues accessing your gallery, please
                <a href="https://fotosfolio.com/contact" style="color:#8b0d0d;text-decoration:none;">contact us</a>.
              </p>
            </div>

            <p style="margin-top:40px;color:#444;font-size:14px;">Warm regards,</p>
            <p style="font-weight:600;color:#111;">The FotosFolio Team & ${photographerName}</p>
          </div>

          <!-- Footer -->
          <div style="border-top:1px solid #eaeaea;background-color:#fafafa;text-align:center;padding:15px;font-size:12px;color:#777;">
            <p style="margin:0;line-height:1.6;">
              © 2025 Fotosfolio. All rights reserved.<br>
              <a href="https://fotosfolio.com/privacy" style="color:#7b1717; text-decoration:none; margin:0 6px;">Privacy Policy</a> •
              <a href="https://fotosfolio.com/terms-and-conditions" style="color:#7b1717; text-decoration:none; margin:0 6px;">Terms of Service</a> •
              <a href="https://fotosfolio.com/contact-us" style="color:#7b1717; text-decoration:none; margin:0 6px;">Contact Support</a>
            </p>
          </div>

        </div>
      </div>
      `,
    }
  }
}


