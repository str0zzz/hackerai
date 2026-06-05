import { authkitMiddleware } from "@workos-inc/authkit-nextjs";
import { NextRequest, NextResponse, NextFetchEvent } from "next/server";
import { isRateLimitError } from "@/lib/api/response";
import {
  REFERRAL_COOKIE_CREATED_AT_NAME,
  REFERRAL_COOKIE_NAME,
  getReferralRewardConfig,
  isValidReferralCode,
} from "@/lib/referrals/config";

const UNAUTHENTICATED_PATHS = new Set([
  "/",
  "/login",
  "/signup",
  "/signup/auth",
  "/logout",
  "/api/clear-auth-cookies",
  "/api/auth/desktop-callback",
  "/api/extra-usage/webhook",
  "/api/fraud/webhook",
  "/api/subscription/webhook",
  "/api/workos/webhook",
  "/callback",
  "/desktop-login",
  "/desktop-callback",
  "/auth-error",
  "/privacy-policy",
  "/terms-of-service",
  "/download",
  "/manifest.json",
]);

function getRedirectUri(): string | undefined {
  if (process.env.VERCEL_ENV === "preview" && process.env.VERCEL_URL) {
    return `https://${process.env.VERCEL_URL}/callback`;
  }
  return undefined;
}

function isDesktopApp(request: NextRequest): boolean {
  const userAgent = request.headers.get("user-agent") || "";
  return userAgent.includes("HackerAI-Desktop");
}

function isUnauthenticatedPath(pathname: string): boolean {
  if (UNAUTHENTICATED_PATHS.has(pathname)) {
    return true;
  }
  if (pathname.startsWith("/share/")) {
    return true;
  }
  if (pathname.startsWith("/invite/")) {
    return true;
  }
  return false;
}

function isBrowserRequest(request: NextRequest): boolean {
  const accept = request.headers.get("accept") ?? "";
  return accept.includes("text/html");
}

const SESSION_HEADER = "x-workos-session";

function withReferralCookie(
  request: NextRequest,
  response: NextResponse,
): NextResponse {
  const referralCode =
    request.nextUrl.searchParams.get("referral_code") ??
    request.nextUrl.searchParams.get("ref");
  if (!referralCode || !isValidReferralCode(referralCode)) return response;

  const config = getReferralRewardConfig();
  if (!config.enabled) return response;

  const cookieOptions = {
    httpOnly: true,
    secure: process.env.NODE_ENV === "production",
    sameSite: "lax" as const,
    maxAge: config.cookieMaxAgeSeconds,
    path: "/",
  };

  response.cookies.set(REFERRAL_COOKIE_NAME, referralCode, cookieOptions);
  response.cookies.set(
    REFERRAL_COOKIE_CREATED_AT_NAME,
    String(Date.now()),
    cookieOptions,
  );

  return response;
}

export default async function middleware(request: NextRequest, event: NextFetchEvent) {
  const middlewareInstance = authkitMiddleware({
    redirectUri: getRedirectUri(),
    eagerAuth: true,
  });

  return middlewareInstance(request, event);
}

export const config = {
  matcher: [
    "/((?!_next|[^?]*\\.(?:html?|css|js(?!on)|jpe?g|webp|png|gif|svg|ttf|woff2?|ico|csv|docx?|xlsx?|zip|webmanifest)).*)",
    "/(api|trpc)(.*)",
  ],
};
