import { GraphQLClient } from 'graphql-request';
import { OrderDirection, getSdk } from 'src/generated/graphql';
import { PrismaService } from 'src/prisma/prisma.service';
import { QUEUE_NAME_MARKETPLACE_STATUS } from 'src/constants/Job.constant';
import { Processor } from '@nestjs/bull';
import { OnModuleInit } from '@nestjs/common';
import { logger } from 'src/commons';
import { Cron, CronExpression } from '@nestjs/schedule';
import { CONTRACT_TYPE, SELL_STATUS, Prisma } from '@prisma/client';
@Processor(QUEUE_NAME_MARKETPLACE_STATUS)
export class MarketplaceStatusProcessor implements OnModuleInit {
  private readonly endpoint = process.env.SUBGRAPH_URL;

  constructor(private readonly prisma: PrismaService) {}

  private getGraphqlClient() {
    return new GraphQLClient(this.endpoint);
  }
  private client = this.getGraphqlClient();

  private sdk = getSdk(this.client);

  async onModuleInit() {
    logger.info(`call First time22`); // Run the task once immediately upon service start
    await this.handleSyncMarketPlaceStatus();
  }

  @Cron(CronExpression.EVERY_10_SECONDS)
  async callEach10SecondSyncMarketplaceStatus() {
    try {
      logger.info(`call per 10 seconds`); // Run the task once immediately upon service start
      await this.handleSyncMarketPlaceStatus();
    } catch (error) {
      logger.error(
        `Sync data marketplace Fail 10 seconds: ${JSON.stringify(error)}`,
      );
    }
  }

  async handleSyncMarketPlaceStatus() {
    try {
      await Promise.all([
        this.handleSyncMarketPlaceStatus721(),
        this.handleSyncMarketPlaceStatus1155(),
      ]);
    } catch (error) {
      logger.error(`Sync data marketplace Fail: ${JSON.stringify(error)}`);
    }
  }

  async handleSyncMarketPlaceStatus721() {
    try {
      const lastItem = await this.getLastSyncedItem(CONTRACT_TYPE.ERC721);

      let skip = 0;
      const first = 1000;
      let hasMore = true;
      let lastProcessedTimestamp = 0;
      // Nếu syncDataStatus là true, tức là quá trình sync đang chạy, không thực hiện gì cả
      if (lastItem && lastItem.syncDataStatus === true) {
        logger.info('Sync data marketplace status 721s is already running');
        return;
      }

      // Đặt syncDataStatus là true để chỉ ra rằng quá trình sync đang chạy
      await this.updateSyncStatus(CONTRACT_TYPE.ERC721, true, 0);

      while (hasMore) {
        const variables = {
          first,
          skip,
          orderDirection: OrderDirection.Asc,
          timestamp: lastItem?.timestamp || 0,
        };

        const response = await this.sdk.GetMarketplaceStatus721(variables);
        if (
          response &&
          response.marketEvent721S &&
          response.marketEvent721S.length > 0
        ) {
          await this.processMarketEvents721(response.marketEvent721S);
          lastProcessedTimestamp = parseInt(
            response.marketEvent721S[response.marketEvent721S.length - 1]
              .timestamp,
          );
          skip += first;
        } else {
          hasMore = false;
        }
      }

      if (lastProcessedTimestamp > 0) {
        await this.updateSyncStatus(
          CONTRACT_TYPE.ERC721,
          false,
          lastProcessedTimestamp,
        );
      } else {
        await this.updateSyncStatus(CONTRACT_TYPE.ERC721, false);
      }
      logger.info('Sync data marketplace status 721s successful');
    } catch (error) {
      console.error(error);
      logger.error(
        `Sync data marketplace status 721s Fail: ${JSON.stringify(error)}`,
      );
    }
  }

  async handleSyncMarketPlaceStatus1155() {
    try {
      const lastItem = await this.getLastSyncedItem(CONTRACT_TYPE.ERC1155);

      let skip = 0;
      const first = 1000;
      let hasMore = true;
      let lastProcessedTimestamp = 0;
      // Nếu syncDataStatus là true, tức là quá trình sync đang chạy, không thực hiện gì cả
      if (lastItem && lastItem.syncDataStatus === true) {
        logger.info('Sync data marketplace status 1155s is already running');
        return;
      }

      // Đặt syncDataStatus là true để chỉ ra rằng quá trình sync đang chạy
      await this.updateSyncStatus(CONTRACT_TYPE.ERC1155, true);
      while (hasMore) {
        const variables = {
          first,
          skip,
          orderDirection: OrderDirection.Asc,
          timestamp: lastItem?.timestamp || 0,
        };
        const response = await this.sdk.GetMarketplaceStatus1155(variables);
        if (
          response &&
          response.marketEvent1155S &&
          response.marketEvent1155S.length > 0
        ) {
          await this.processMarketEvents1155(response.marketEvent1155S);
          lastProcessedTimestamp = parseInt(
            response.marketEvent1155S[response.marketEvent1155S.length - 1]
              .timestamp,
          );
          skip += first;
        } else {
          hasMore = false;
        }
      }
      if (lastProcessedTimestamp > 0) {
        await this.updateSyncStatus(
          CONTRACT_TYPE.ERC1155,
          false,
          lastProcessedTimestamp,
        );
      } else {
        await this.updateSyncStatus(CONTRACT_TYPE.ERC1155, false);
      }

      logger.info('Sync data marketplace status 1155s successful');
    } catch (error) {
      logger.error(`Sync data marketplace status 1155s Fail: ${error}`);
    }
  }

  async processMarketEvents721(events) {
    Promise.all(
      events.map(async (item) => {
        if (item.nftId) {
          const nft = await this.getNFT(item.nftId.tokenId, item?.address);
          if (nft) {
            const timestamp = parseInt(item.timestamp);
            if (item.event === SELL_STATUS.AskNew) {
              await this.createMarketplaceStatus721(nft, item, timestamp);
            } else if (
              item.event === SELL_STATUS.AskCancel ||
              item.event === SELL_STATUS.Trade
            ) {
              await this.deleteMarketplaceStatus(
                nft,
                item,
                CONTRACT_TYPE.ERC721,
              );
            }
          }
        }
      }),
    );
  }

  async processMarketEvents1155(events) {
    await Promise.all(
      events.map(async (item) => {
        if (item.nftId) {
          const nft = await this.getNFT(item.nftId.tokenId, item?.address);
          if (nft) {
            const timestamp = parseInt(item.timestamp);
            if (item.event === SELL_STATUS.AskNew) {
              await this.createMarketplaceStatus1155(nft, item, timestamp);
            } else if (
              item.event === SELL_STATUS.AskCancel ||
              item.event === SELL_STATUS.Trade
            ) {
              await this.deleteMarketplaceStatus(
                nft,
                item,
                CONTRACT_TYPE.ERC1155,
              );
            }
          }
        }
      }),
    );
  }

  async createMarketplaceStatus721(nft, item, timestamp) {
    const whereCondition: Prisma.MarketplaceStatusWhereInput = {
      tokenId: nft.id,
      collectionId: nft.collectionId,
    };
    const existingMarketplaceStatus =
      await this.prisma.marketplaceStatus.findFirst({
        where: whereCondition,
      });
    if (!existingMarketplaceStatus) {
      await this.prisma.marketplaceStatus.create({
        data: {
          tokenId: nft.id,
          collectionId: nft.collectionId,
          quoteToken: item.quoteToken,
          timestamp,
          price: this.weiToEther(item.price || 0),
          priceWei: `${item.price || 0}`,
          netPrice: this.weiToEther(item.netPrice || 0),
          netPriceWei: `${item.netPrice || 0}`,
          event: item.event,
          quantity: 1,
          operation: item?.operation,
          operationId: item?.operationId,
          txHash: item?.txHash,
          from: item?.from,
          askId: item?.id,
        },
      });
    }
  }

  async createMarketplaceStatus1155(nft, item, timestamp) {
    const whereCondition: Prisma.MarketplaceStatusWhereInput = {
      // price: parseInt(item?.price),
      tokenId: nft.id,
      collectionId: nft.collectionId,
      operationId: item?.operationId,
    };
    const existingMarketplaceStatus =
      await this.prisma.marketplaceStatus.findFirst({
        where: whereCondition,
      });
    if (!existingMarketplaceStatus) {
      await this.prisma.marketplaceStatus.create({
        data: {
          tokenId: nft.id,
          collectionId: nft.collectionId,
          quoteToken: item.quoteToken,
          timestamp,
          price: this.weiToEther(item.price || 0),
          priceWei: `${item.price || 0}`,
          netPrice: this.weiToEther(item.netPrice || 0),
          netPriceWei: `${item.netPrice || 0}`,
          event: item.event,
          quantity: parseInt(item?.quantity),
          operation: item?.operation,
          operationId: item?.operationId,
          txHash: item?.txHash,
          from: item?.from,
          askId: item?.id,
        },
      });
    } else {
      await this.prisma.marketplaceStatus.update({
        where: { id: existingMarketplaceStatus.id },
        data: {
          quoteToken: item.quoteToken,
          timestamp,
          price: this.weiToEther(item.price || 0),
          priceWei: `${item.price || 0}`,
          netPrice: this.weiToEther(item.netPrice || 0),
          netPriceWei: `${item.netPrice || 0}`,
          event: item.event,
          quantity: parseInt(item?.quantity),
          operation: item?.operation,
          operationId: item?.operationId,
          txHash: item?.txHash,
          from: item?.from,
          askId: item?.id,
        },
      });
    }
  }

  async getNFT(tokenId: string, address: string) {
    const collection = await this.prisma.collection.findFirst({
      where: {
        address: address,
      },
    });

    if (collection) {
      if (!collection?.isU2U) {
        return await this.prisma.nFT.findUnique({
          where: {
            id_collectionId: {
              id: tokenId,
              collectionId: collection.id,
            },
          },
        });
      } else {
        return await this.prisma.nFT.findFirst({
          where: {
            AND: [{ u2uId: tokenId }, { collectionId: collection.id }],
          },
        });
      }
    }
  }

  async deleteMarketplaceStatus(nft, item, type: CONTRACT_TYPE) {
    const whereCondition: Prisma.MarketplaceStatusWhereInput =
      type == CONTRACT_TYPE.ERC1155
        ? {
            // price: parseInt(item?.price),
            tokenId: nft.id,
            collectionId: nft.collectionId,
            operationId: item?.operationId,
          }
        : {
            tokenId: nft.id,
            collectionId: nft.collectionId,
          };
    const existingMarketplaceStatus =
      await this.prisma.marketplaceStatus.findFirst({
        where: whereCondition,
      });

    if (existingMarketplaceStatus) {
      await this.prisma.marketplaceStatus.delete({
        where: {
          id: existingMarketplaceStatus.id,
        },
      });
    }
  }

  async getLastSyncedItem(type: CONTRACT_TYPE) {
    return await this.prisma.syncMasterData.findFirst({
      where: {
        type: type,
      },
      orderBy: {
        timestamp: 'desc',
      },
    });
  }
  weiToEther(wei) {
    return wei / 1000000000000000000; // 1 Ether = 10^18 Wei
  }
  async updateSyncStatus(
    contractType: CONTRACT_TYPE,
    syncDataStatus: boolean,
    timestamp = null,
  ) {
    const dataUpdate: Prisma.SyncMasterDataCreateInput = {
      type: contractType,
      syncDataStatus: syncDataStatus,
    };
    const dataCreate: Prisma.SyncMasterDataCreateInput = {
      type: contractType,
      syncDataStatus: syncDataStatus,
    };
    if (timestamp !== null && timestamp > 0) {
      dataUpdate.timestamp = timestamp;
    }
    if (timestamp === 0) {
      dataCreate.timestamp = 0;
    }

    await this.prisma.syncMasterData.upsert({
      where: { type: contractType },
      update: dataUpdate,
      create: dataCreate,
    });
  }
}
