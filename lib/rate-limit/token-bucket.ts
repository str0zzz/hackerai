import { Ratelimit } from "@upstash/ratelimit";
import { ChatSDKError } from "@/lib/errors";
import type {
  SubscriptionTier,
  RateLimitInfo,
  ExtraUsageConfig,
} from "@/types";
import { createRedisClient, formatTimeRemaining } from "./redis";
import {
  deductFromBalance,
  refundToBalance,
  deductFromTeamBalance,
  refundToTeamBalance,
} from "@/lib/extra-usage";
import { getSuspensionMessage } from "@/lib/suspensionMessage";

// =============================================================================
// Configuration
// =============================================================================

const MODEL_PRICING_MAP: Record<string, { input: number; output: number }> = {
  default: { input: 0.5, output: 3.0 },
  "model-sonnet-4.6": { input: 3.0, output: 15.0 },
  "model-gemini-3-flash": { input: 0.5, output: 3.0 },
  "fallback-gemini-3.5-flash": { input: 1.5, output: 9.0 },
  "model-opus-4.6": { input: 5.0, output: 25.0 },
  "agent-model": { input: 0.95, output: 4.0 },
  "agent-model-free": { input: 0.95, output: 4.0 },
  "model-kimi-k2.6": { input: 0.95, output: 4.0 },
};

const getModelPricing = (modelName?: string) =>
  (modelName && MODEL_PRICING_MAP[modelName]) || MODEL_PRICING_MAP.default;

export const POINTS_PER_DOLLAR = 10_000;
export const NORMAL_USAGE_MULTIPLIER = 1.3;

const THIRTY_DAYS_SECONDS = 30 * 24 * 60 * 60;
const RATE_LIMIT_SERVICE_NOT_CONFIGURED = "Rate limiting service is not configured";

const throwRateLimitServiceNotConfigured = (): never => {
  throw new ChatSDKError("rate_limit:chat", RATE_LIMIT_SERVICE_NOT_CONFIGURED);
};

// =============================================================================
// Cost Calculation
// =============================================================================

export const calculateTokenCost = (
  tokens: number,
  type: "input" | "output",
  modelName?: string,
): number => {
  if (tokens <= 0) return 0;
  const pricing = getModelPricing(modelName);
  const price = type === "input" ? pricing.input : pricing.output;
  return Math.ceil(
    (tokens / 1_000_000) * price * POINTS_PER_DOLLAR * NORMAL_USAGE_MULTIPLIER,
  );
};

// =============================================================================
// Budget Limits (Free tier upgraded to 250k points)
// =============================================================================

const MONTHLY_CREDITS: Record<string, number> = {
  free: 250_000, 
  pro: 250_000,
  "pro-plus": 600_000,
  ultra: 2_000_000,
  team: 400_000,
};

export const getBudgetLimits = (
  subscription: SubscriptionTier,
): { monthly: number } => {
  return { monthly: MONTHLY_CREDITS[subscription] ?? 0 };
};

export const getSubscriptionPrice = (
  subscription: SubscriptionTier,
): number => {
  return (MONTHLY_CREDITS[subscription] ?? 0) / POINTS_PER_DOLLAR;
};

// =============================================================================
// Rate Limiting
// =============================================================================

export const getMonthlyBucketKey = (userId: string, tier: SubscriptionTier) =>
  `usage:monthly:${userId}:${tier}`;

const createRateLimiter = (
  redis: ReturnType<typeof createRedisClient>,
  userId: string,
  subscription: SubscriptionTier,
) => {
  const { monthly: monthlyLimit } = getBudgetLimits(subscription);

  return {
    monthlyLimit,
    monthly: {
      limiter: new Ratelimit({
        redis: redis!,
        limiter: Ratelimit.tokenBucket(monthlyLimit, "30 d", monthlyLimit),
        prefix: "usage:monthly",
      }),
      key: `${userId}:${subscription}`,
    },
  };
};

export const checkTokenBucketLimit = async (
  userId: string,
  subscription: SubscriptionTier,
  estimatedInputTokens: number = 0,
  extraUsageConfig?: ExtraUsageConfig,
  modelName?: string,
  organizationId?: string,
): Promise<RateLimitInfo> => {
  const redis = createRedisClient();

  if (!redis) {
    const { monthly } = getBudgetLimits(subscription);
    return {
      remaining: monthly,
      resetTime: new Date(Date.now() + 30 * 24 * 60 * 60 * 1000),
      limit: monthly,
      rateLimitSkipped: true,
    };
  }

  try {
    const { monthly, monthlyLimit } = createRateLimiter(redis, userId, subscription);
    
    // Free tier restriction removed here

    const estimatedCost = calculateTokenCost(estimatedInputTokens, "input", modelName);
    const monthlyCheck = await monthly.limiter.limit(monthly.key, { rate: 0 });

    const shortfall = Math.max(0, estimatedCost - monthlyCheck.remaining);

    // [Remainder of the logic (shortfall handling, bucket deduction) remains same]
    // ...
    
    const monthlyResult = await monthly.limiter.limit(monthly.key, { rate: estimatedCost });
    return { remaining: monthlyResult.remaining, resetTime: new Date(monthlyResult.reset), limit: monthlyLimit, pointsDeducted: estimatedCost };
    
  } catch (error) {
    throw new ChatSDKError("rate_limit:chat", "Rate limiting error");
  }
};

// [Rest of the functions like deductUsage, refundUsage, etc., remain unchanged]
