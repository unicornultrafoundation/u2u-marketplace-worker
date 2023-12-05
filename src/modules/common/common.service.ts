import { Injectable } from '@nestjs/common';
import { create } from 'ipfs-http-client';
import { PrismaService } from 'src/prisma/prisma.service';

@Injectable()
export class CommonService {
  private ipfs;
  constructor() {
    this.ipfs = create({
      host: process.env.IPFS_URL,
      port: parseInt(process.env.IPFS_PORT),
      protocol: process.env.IPFS_PROTOCOL,
    });
  }

  async uploadIpfs(files: Express.Multer.File[], metadata: any) {
    try {
      const fileResults = await Promise.all(
        files.map((file) => this.ipfs.add(file.buffer)),
      );
      const fileHashes = fileResults.map((result) => result.path);
      if (metadata) {
        const metadataObject = JSON.parse(metadata);
        const updatedMetadata = { ...metadataObject, fileHashes };

        const metadataResult = await this.ipfs.add(
          JSON.stringify(updatedMetadata),
        );
        return {
          fileHashes: fileHashes,
          metadataHash: metadataResult.path,
        };
      } else {
        return { fileHashes: fileHashes };
      }
    } catch (err) {
      console.log('err: ', err);
    }
  }

  async getFromIpfs(hash: string): Promise<{ data: any; type: string }> {
    try {
      const content = [];
      for await (const chunk of this.ipfs.cat(hash)) {
        content.push(chunk);
      }
      const buffer = Buffer.concat(content);

      // Try to parse buffer as JSON
      try {
        const json = JSON.parse(buffer.toString());
        return { data: json, type: 'json' };
      } catch (e) {
        // If it's not JSON, return it as a file
        return { data: buffer, type: 'file' };
      }
    } catch (err) {
      console.error('Error retrieving content from IPFS:', err);
      throw err;
    }
  }
}
