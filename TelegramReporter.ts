import { User } from "@schemas";
import { CronJob } from "cron";
import { logger } from "../utils/logger";
import Redis from "ioredis";
import { IUser } from "@apps/users/user";
import { Queue } from "bullmq";
import { Singleton } from "@providers";
import { TIME_FRAMES, TOKEN_AGES } from "../../const/const";
import { getTopDataByField } from "@bot/utils";
import {
  smartMoneyNetflowTemplate,
  topFreshPoolsTemplate,
  topHotLPsTemplate,
} from "@bot/utils/template";
import { config } from "@bot/config";
import { QUEUE_NAMES } from "@bot/constants";

const redisOptions = {
  host: process.env.REDIS_HOST || "localhost",
  port: parseInt(process.env.REDIS_PORT || "6379", 10),
  username: process.env.REDIS_USER || undefined,
  password: process.env.REDIS_PASSWORD || undefined,
};

// QUEUE SETUP
const queues = {
  radarReports: new Queue(QUEUE_NAMES.radarReports, {
    connection: redisOptions,
    prefix: config.BOT_PREFIX,
  }),
};

// UTILITIES
const addJobToRadarReportsQueue = (job: WorkerJob) =>
  queues.radarReports.add(job.type, job, {
    removeOnComplete: true,
    removeOnFail: true,
  });

// Create a Redis client
const redisClient = new Redis(redisOptions);

const getDataFromCacheOrAPI = async <T>(
  cacheKey: string,
  apiFunction: () => Promise<T>,
  cacheExpiry: number
) => {
  const dataFromCache = await redisClient.get(cacheKey);
  let data;

  if (dataFromCache) {
    try {
      data = JSON.parse(dataFromCache);
    } catch (err) {
      // Handle parsing error if necessary
    }
  } else {
    data = await apiFunction();

    if (data) {
      // Cache the fresh data
      redisClient.setex(cacheKey, cacheExpiry, JSON.stringify(data));
    }
  }

  return data;
};

const getBufferFromCacheOrAPI = async (
  cacheKey: string,
  apiFunction: () => Promise<Buffer | null>,
  cacheExpiry: number
) => {
  const dataFromCache = await redisClient.get(cacheKey);
  let data: Buffer | null = null;

  if (dataFromCache) {
    try {
      data = Buffer.from(dataFromCache, "base64"); // Parse the cached data as a base64 encoded string
    } catch (err) {
      // Handle parsing error if necessary
    }
  } else {
    data = await apiFunction();

    if (data) {
      // Cache the fresh data
      const base64Data = data.toString("base64");
      redisClient.setex(cacheKey, cacheExpiry, base64Data);
    }
  }

  return data;
};


// Exchanges
// const getExchangeNetflowTokenAgeFromCacheOrAPI = async (
//   time: string,
//   token_age: string
// ): Promise<IExchangeNetflow[] | undefined> => {
//   const cacheKey = `${config.BOT_PREFIX}:sumexchangenetflow:${time}:${token_age}`;
//   const tokenInfo = await getDataFromCacheOrAPI<IExchangeNetflow[] | undefined>(
//     cacheKey,
//     async () => {
//       // Call your API or fetch fresh data here
//       return Singleton.getWalletService().getExchangeNetflowTokenAge(
//         time,
//         token_age
//       );
//     },
//     // 3600 // 1h
//     7200 // 2h
//   );
//   return tokenInfo;
// };

// Smart Money
const getSmartMoneyNetflowTokenAgeFromCacheOrAPI = async (
  time: string,
  token_age: string
): Promise<ISmartMoneyNetflow[] | undefined> => {
  const cacheKey = `${config.BOT_PREFIX}:sumsmnetflowtokenage:${time}-${token_age}`;
  const tokenInfo = await getDataFromCacheOrAPI<
    ISmartMoneyNetflow[] | undefined
  >(
    cacheKey,
    async () => {
      // Call your API or fetch fresh data here
      return Singleton.getWalletService().getSmartMoneyNetflowTokenAge(
        time,
        token_age
      );
    },
    // 3600 // 1h
    7200 // 2h
  );
  return tokenInfo;
};

// Fresh wallet
const getTopFreshPoolsTokenAgeFromCacheOrAPI = async (
  time: string,
  token_age: string
): Promise<ITopFreshPool[] | undefined> => {
  const cacheKey = `${config.BOT_PREFIX}:sumtopfreshpools:${time}:${token_age}`;
  const tokenInfo = await getDataFromCacheOrAPI<ITopFreshPool[] | undefined>(
    cacheKey,
    async () => {
      // Call your API or fetch fresh data here
      return Singleton.getWalletService().getTopFreshPoolsTokenAge(
        time,
        token_age
      );
    },
    // 3600 // 1h
    7200 // 2h
  );
  return tokenInfo;
};

// Hot pools
const getHotLPsTokenAgeFromCacheOrAPI = async (
  time: string,
  token_age: string
): Promise<IHotLPs[] | undefined> => {
  const cacheKey = `${config.BOT_PREFIX}:sumhotlps:${time}:${token_age}`;
  const tokenInfo = await getDataFromCacheOrAPI<IHotLPs[] | undefined>(
    cacheKey,
    async () => {
      // Call your API or fetch fresh data here
      return Singleton.getWalletService().getHotLPsTokenAge(time, token_age);
    },
    // 300 // 5m
    7200 // 2h
  );
  return tokenInfo;
};

// const getExchangeMessage = async (time: string, token_age: string, locale?: string) => {
//   const exchangeNetflow =
//     (await getExchangeNetflowTokenAgeFromCacheOrAPI(time, token_age)) || [];

//   const imageCacheKey = `${config.BOT_PREFIX}:sumexchangenetflowchart:${time}:${token_age}:${locale}`;

//   const imageChart: Buffer | null = await getBufferFromCacheOrAPI(
//     imageCacheKey, // Use a unique cache key for each chart
//     async () => {
//       // Call your API or fetch fresh data here
//       return Singleton.getUserService().getChartImage(
//         `${config.NERD_API_BASE_URL}/chart/summary/token-age/exchange-flow?interval=${time}&token_age=${token_age}`
//       );
//     },
//     // 3600 // 1h
//     7200 // 2h
//   );

//   if (exchangeNetflow) {
//     const topDeposit = getTopDataByField(
//       exchangeNetflow,
//       "usd_net",
//       5,
//       true
//     );

//     const topWithdraw = getTopDataByField(
//       exchangeNetflow,
//       "usd_net",
//       5,
//       false
//     );

//     const topDepositReal = topDeposit.filter(
//       (p) => p.usd_net !== null && p.usd_net >= 0
//     );
//     const topWithdrawReal = topWithdraw.filter(
//       (l) => l.usd_net !== null && l.usd_net <= 0
//     );

//     const message = exchangeNetflowTemplate({
//       time,
//       tokenAge: token_age,
//       topDeposit: topDepositReal,
//       topWithdraw: topWithdrawReal,
//       locale,
//     });

//     return {
//       image: imageCacheKey,
//       message,
//     };
//   }
// };

const getSmartMoneyMessage = async (time: string, token_age: string, locale?: string) => {
  let chain = 'avax'
  const smartMoneyNetflow =
    (await getSmartMoneyNetflowTokenAgeFromCacheOrAPI(time, token_age)) || [];

  const imageCacheKey = `${config.BOT_PREFIX}:sumsmnetflowchart:${time}:${token_age}`;

  const imageChart: Buffer | null = await getBufferFromCacheOrAPI(
    `${config.BOT_PREFIX}:sumsmnetflowchart:${time}:${token_age}`, // Use a unique cache key for each chart
    async () => {
      // Call your API or fetch fresh data here
      return Singleton.getUserService().getChartImage(
        `${config.NERD_API_BASE_URL}/chart/summary/token-age/smart-money?&chain=${chain}&interval=${time}&token_age=${token_age}`
      );
    },
    // 3600 // 1h
    7200 // 2h
  );

  const topBuy = getTopDataByField(smartMoneyNetflow, "usd_net", 5, true);

  const topSell = getTopDataByField(smartMoneyNetflow, "usd_net", 5, false);

  const topBuyReal = topBuy.filter(
    (p) => p.usd_net !== null && p.usd_net >= 0
  );
  const topSellReal = topSell.filter(
    (l) => l.usd_net !== null && l.usd_net <= 0
  );

  const message = smartMoneyNetflowTemplate({
    time,
    token_age,
    topBuy: topBuyReal,
    topSell: topSellReal,
    locale,
  });

  return {
    image: imageCacheKey,
    message,
  };
};

const getFreshWalletMessage = async (time: string, token_age: string, locale: string) => {
  let chain = 'avax'
  const topFreshPools =
    (await getTopFreshPoolsTokenAgeFromCacheOrAPI(time, token_age)) || [];

  const imageCacheKey = `${config.BOT_PREFIX}:sumtopfreshpoolschart:${time}:${token_age}`;

  const imageChart: Buffer | null = await getBufferFromCacheOrAPI(
    imageCacheKey, // Use a unique cache key for each chart
    async () => {
      // Call your API or fetch fresh data here
      return Singleton.getUserService().getChartImage(
        `${config.NERD_API_BASE_URL}/chart/summary/token-age/fresh-wallet?chain=${chain}&interval=${time}&token_age=${token_age}`
      );
    },
    // 3600 // 1h
    7200 // 2h
  );

  const message = topFreshPoolsTemplate({
    time,
    tokenAge: token_age,
    topPools: topFreshPools.slice(0, 10),
    locale,
  });

  return {
    image: imageCacheKey,
    message,
  };
};

const getTopPoolsMessage = async (time: string, token_age: string, locale?: string) => {
  let chain = 'avax'
  const topHotLPs =
    (await getHotLPsTokenAgeFromCacheOrAPI(time, token_age)) || [];
  const imageCacheKey = `${config.BOT_PREFIX}:sumhotlpschart:${time}:${token_age}`;

  const imageChart: Buffer | null = await getBufferFromCacheOrAPI(
    imageCacheKey, // Use a unique cache key for each chart
    async () => {
      // Call your API or fetch fresh data here
      return Singleton.getUserService().getChartImage(
        `${config.NERD_API_BASE_URL}/chart/summary/token-age/hot-lp?chain=${chain}&interval=${time}&token_age=${token_age}`
      );
    },
    // 3600 // 1h
    7200 // 2h
  );

  const message = topHotLPsTemplate({
    time,
    tokenAge: token_age,
    topHotLPs: topHotLPs.slice(0, 10),
    locale,
  });

  return {
    image: imageCacheKey,
    message,
  };
};

const cacheResults = async (time: string, tokenAge: string) => {
  // Call the functions and cache the results in parallel
  await Promise.all([
    // getExchangeNetflowTokenAgeFromCacheOrAPI(time, tokenAge),
    getSmartMoneyNetflowTokenAgeFromCacheOrAPI(time, tokenAge),
    getTopFreshPoolsTokenAgeFromCacheOrAPI(time, tokenAge),
    getHotLPsTokenAgeFromCacheOrAPI(time, tokenAge),
  ]);
};

// Function to send results to users
const sendResultsToUsers = async (time: string, tokenAge: string) => {
  const userItems: IUser[] = await Singleton.getUserService().getUsersFromCacheOrDatabase();
  // Fix default locale
  const locale = 'en';
  for (const user of userItems) {
    const radarReports = user.radarReports || [];
    let telegramIds: string[] = [String(user.telegramId)];
    const radarReportChannels = user.radarReportChannels || [];
    if (radarReportChannels.length > 0) {
      telegramIds = radarReportChannels.map((ch) => String(ch));
    }

    for (const radarReport of radarReports) {
      if (radarReport.timeFrame === time && radarReport.tokenAge === tokenAge) {
        let messageData;
        switch (radarReport.category) {
          // case "exchange":
          //   messageData = await getExchangeMessage(
          //     radarReport.timeFrame,
          //     radarReport.tokenAge,
          //     locale,
          //   );
          //   break;
          case "smartMoney":
            messageData = await getSmartMoneyMessage(
              radarReport.timeFrame,
              radarReport.tokenAge,
              locale,
            );
            break;
          case "freshWallet":
            messageData = await getFreshWalletMessage(
              radarReport.timeFrame,
              radarReport.tokenAge,
              locale,
            );
            break;
          case "hotPool":
            messageData = await getTopPoolsMessage(
              radarReport.timeFrame,
              radarReport.tokenAge,
              locale,
            );
            break;
          default:
            break;
        }

        if (messageData) {
          addJobToRadarReportsQueue({
            type: "RadarReportJob",
            data: {
              userId: user._id,
              telegramIds: telegramIds,
              message: messageData.message || "",
              image: messageData.image || "",
            },
          });
        }
      }
    }
  }
};

export const radaReporter = async () => {
  try {
    logger.info("[radarReporter] is started");
    for (const time of TIME_FRAMES) {
      const cronTime = `0 */${time.replace("h", "")} * * *`;
      const cron = new CronJob(cronTime, async () => {
        logger.info(`Caching results and sending to users for time: ${time}`);
        for (const tokenAge of TOKEN_AGES) {
          await cacheResults(time, tokenAge);
          await sendResultsToUsers(time, tokenAge);
        }
      });
      cron.start();
    }
  } catch (err) {
    logger.error("[radarReporter] error", err);
  }
};
