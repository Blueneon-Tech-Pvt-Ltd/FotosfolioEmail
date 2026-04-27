import { AddonExpiryEmailDto } from 'src/cron-jobs/dto/addonexpiry.dto';
import { ContactDto } from 'src/user/dto/contact.dto';

// Email Categories - 6 worker queues for microservice
export enum EmailCategory {
  ACCOUNT = 'account',
  SUBSCRIPTION = 'subscription',
  SECURITY = 'security',
  PROJECT = 'project',
  PAYMENT = 'payment',
  STORAGE = 'storage',
}

// Email Types - Microservice format (maps to categories)
export enum EmailType {
  // Account
  ACCOUNT_CREATED = 'ACCOUNT_CREATED',
  ACCOUNT_ACTIVATION = 'ACCOUNT_ACTIVATION',
  PASSWORD_RESET = 'PASSWORD_RESET',
  PASSWORD_RESET_SUCCESS = 'PASSWORD_RESET_SUCCESS',
  EMAIL_VERIFICATION = 'EMAIL_VERIFICATION',
  PASSWORD_CHANGED = 'PASSWORD_CHANGED',
  EMAIL_CHANGED = 'EMAIL_CHANGED',
  ACCOUNT_DELETED = 'ACCOUNT_DELETED',
  TWO_FACTOR_ENABLED = 'TWO_FACTOR_ENABLED',
  TWO_FACTOR_CODE = 'TWO_FACTOR_CODE',
  CONTACT_FORM = 'CONTACT_FORM',

  // Subscription
  SUBSCRIPTION_STARTED = 'SUBSCRIPTION_STARTED',
  SUBSCRIPTION_RENEWED = 'SUBSCRIPTION_RENEWED',
  SUBSCRIPTION_CANCELLED = 'SUBSCRIPTION_CANCELLED',
  SUBSCRIPTION_EXPIRING = 'SUBSCRIPTION_EXPIRING',
  SUBSCRIPTION_EXPIRED = 'SUBSCRIPTION_EXPIRED',
  PLAN_UPGRADED = 'PLAN_UPGRADED',
  PLAN_DOWNGRADED = 'PLAN_DOWNGRADED',

  // Security
  LOGIN_ALERT = 'LOGIN_ALERT',
  NEW_IP_LOGIN = 'NEW_IP_LOGIN',
  SUSPICIOUS_ACTIVITY = 'SUSPICIOUS_ACTIVITY',
  GOOGLE_ACCOUNT_DISCONNECTED = 'GOOGLE_ACCOUNT_DISCONNECTED',
  TWO_FA_DISABLED = 'TWO_FA_DISABLED',
  PASSKEY_ENABLED = 'PASSKEY_ENABLED',
  PASSKEY_DISABLED = 'PASSKEY_DISABLED',
  SECURITY_QUESTION_ENABLED = 'SECURITY_QUESTION_ENABLED',
  SECURITY_QUESTION_DISABLED = 'SECURITY_QUESTION_DISABLED',

  // Project
  PROJECT_SHARED = 'PROJECT_SHARED',
  PROJECT_INVITATION = 'PROJECT_INVITATION',
  PROJECT_TRANSFER = 'PROJECT_TRANSFER',
  ACCESS_REQUEST = 'ACCESS_REQUEST',
  FAVOURITE_ACCESS_REQUEST = 'FAVOURITE_ACCESS_REQUEST',
  GALLERY_SHARED = 'GALLERY_SHARED',
  FOLDER_SHARED = 'FOLDER_SHARED',

  // Payment
  PAYMENT_SUCCESS = 'PAYMENT_SUCCESS',
  PAYMENT_FAILED = 'PAYMENT_FAILED',
  PAYMENT_REJECTION = 'PAYMENT_REJECTION',
  REFUND_PROCESSED = 'REFUND_PROCESSED',

  // Storage
  STORAGE_WARNING = 'STORAGE_WARNING',
  STORAGE_FULL = 'STORAGE_FULL',
  ADDON_EXPIRY = 'ADDON_EXPIRY',
  ADDON_FINAL_GRACE = 'ADDON_FINAL_GRACE',
}

// Email Data Interface - Standard output from builder services
// category and type are optional because they are automatically added by the worker pool
export interface EmailData {
  to: string;
  subject: string;
  html: string;
  text: string;
  category?: EmailCategory;
  type?: EmailType;
}

// Legacy enum (keeping for backward compatibility if needed)
export enum EmailJobType {
  // Contact form
  CONTACT = 'contact',

  // User account related
  ACCOUNT_CREATED = 'account-created',
  ACCOUNT_ACTIVATION = 'account-activation',
  PASSWORD_RESET = 'password-reset',
  PASSWORD_RESET_SUCCESS = 'password-reset-success',
  LOGIN_NOTIFICATION = 'login-notification',
  NEW_IP_LOGIN = 'new-ip-login',
  GOOGLE_ACCOUNT_DISCONNECTED = 'google-account-disconnected',

  // OTP related
  OTP_EMAIL = 'otp-email',
  OTP_VERIFIED = 'otp-verified',

  // Subscription related
  SUBSCRIPTION_CREATED = 'subscription-created',
  SUBSCRIPTION_EXPIRATION_REMINDER = 'subscription-expiration-reminder',
  SUBSCRIPTION_EXPIRED = 'subscription-expired',

  // Payment related
  PAYMENT_SUCCESS = 'payment-success',
  PAYMENT_REJECTION = 'payment-rejection',

  // Project related
  PROJECT_INVITATION = 'project-invitation',
  FAVOURITE_ACCESS_REQUEST = 'favourite-access-request',
  ACCESS_REQUEST = 'access-request',
  PROJECT_TRANSFER = 'project-transfer',

  // Storage addon related
  ADDON_EXPIRY = 'addon-expiry',
  ADDON_FINAL_GRACE = 'addon-final-grace',

  // Security notification related
  TWO_FA_ENABLED = '2fa-enabled',
  TWO_FA_DISABLED = '2fa-disabled',
  PASSKEY_ENABLED = 'passkey-enabled',
  PASSKEY_DISABLED = 'passkey-disabled',
  SECURITY_QUESTION_ENABLED = 'security-question-enabled',
  SECURITY_QUESTION_DISABLED = 'security-question-disabled',
}

// Base email job interface
export interface BaseEmailJob {
  type: EmailJobType;
  payload: any;
}

// Individual job payload interfaces
export interface ContactEmailPayload {
  contact: ContactDto;
}

export interface AccountCreatedPayload {
  to: string;
  username: string;
}

export interface SubscriptionCreatedPayload {
  to: string;
  userName: string;
  planName: string;
  startDate: Date;
  endDate: Date;
  status: string;
}

export interface PaymentSuccessPayload {
  to: string;
  userName: string;
  planName: string;
}

export interface AccountActivationPayload {
  to: string;
  userName: string;
  planName: string;
}

export interface PaymentRejectionPayload {
  to: string;
  userName: string;
  planName: string;
}

export interface OtpEmailPayload {
  to: string;
  userName: string;
  otpCode: string;
}

export interface OtpVerifiedPayload {
  to: string;
  userName: string;
}

export interface SubscriptionExpirationReminderPayload {
  to: string;
  userName: string;
  daysRemaining: string;
}

export interface SubscriptionExpiredPayload {
  to: string;
  userName: string;
  graceDaysRemaining: number;
}

export interface PasswordResetPayload {
  to: string;
  username: string;
  resetLink: string;
}

export interface PasswordResetSuccessPayload {
  to: string;
  username: string;
}

export interface LoginNotificationPayload {
  to: string;
  userName: string;
  loginTime: Date;
  ipAddress?: string;
}

export interface NewIpLoginPayload {
  userEmail: string;
  ipAddress: string;
  device: string;
  location: string;
  datetime: Date;
}

export interface GoogleAccountDisconnectedPayload {
  userEmail: string;
  username: string;
  device?: string;
  location?: string;
  ipAddress?: string;
  datetime: Date;
}

export interface AccessRequestPayload {
  projectName: string;
  requesterEmail: string;
  ownerEmail: string;
  PhotographerName: string;
  projectId: string;
}

export interface ProjectInvitationPayload {
  projectName: string;
  photographerName: string;
  receiverEmail: string;
  invitationLink: string;
}

export interface FavouriteAccessRequestPayload {
  projectName: string;
  requesterEmail: string;
  ownerEmail: string;
  photographerName: string;
  projectId: string;
}

export interface AddonExpiryPayload {
  contact: AddonExpiryEmailDto;
}

export interface AddonFinalGracePayload {
  contact: {
    userEmail: string;
    userName: string;
    graceDaysRemaining: number;
    renewLink: string;
    deleteLink: string;
    storageUsed: number;
  };
}

export interface TwoFAEnabledPayload {
  userEmail: string;
  device: string;
  location: string;
  ipAddress: string;
  datetime: Date;
}

export interface TwoFADisabledPayload {
  userEmail: string;
  device: string;
  location: string;
  ipAddress: string;
  datetime: Date;
}

export interface PasskeyEnabledPayload {
  userEmail: string;
  passkeyName: string;
  device: string;
  location: string;
  ipAddress: string;
  datetime: Date;
}

export interface PasskeyDisabledPayload {
  userEmail: string;
  passkeyName: string;
  device: string;
  location: string;
  ipAddress: string;
  datetime: Date;
}

export interface SecurityQuestionEnabledPayload {
  userEmail: string;
  device: string;
  location: string;
  ipAddress: string;
  datetime: Date;
}

export interface SecurityQuestionDisabledPayload {
  userEmail: string;
  device: string;
  location: string;
  ipAddress: string;
  datetime: Date;
}

export interface ProjectTransferPayload {
  receiverEmail: string;
  receiverName: string;
  senderName: string;
  projectName: string;
  projectLink: string;
  totalFiles: number;
  totalSize: string;
}

// Union type for all possible payloads
export type EmailJobPayload =
  | ContactEmailPayload
  | AccountCreatedPayload
  | SubscriptionCreatedPayload
  | PaymentSuccessPayload
  | AccountActivationPayload
  | PaymentRejectionPayload
  | OtpEmailPayload
  | OtpVerifiedPayload
  | SubscriptionExpirationReminderPayload
  | SubscriptionExpiredPayload
  | PasswordResetPayload
  | PasswordResetSuccessPayload
  | LoginNotificationPayload
  | NewIpLoginPayload
  | GoogleAccountDisconnectedPayload
  | AccessRequestPayload
  | ProjectInvitationPayload
  | AddonExpiryPayload
  | AddonFinalGracePayload
  | TwoFAEnabledPayload
  | TwoFADisabledPayload
  | PasskeyEnabledPayload
  | PasskeyDisabledPayload
  | SecurityQuestionEnabledPayload
  | SecurityQuestionDisabledPayload
  | ProjectTransferPayload
  | FavouriteAccessRequestPayload;

// Email job interface with typed payload
export interface EmailJob extends BaseEmailJob {
  type: EmailJobType;
  payload: EmailJobPayload;
}
