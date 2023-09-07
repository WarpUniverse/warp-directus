import emitter from "../emitter.js";
import type {EventContext} from "@directus/types";
import {AssetsService} from "./assets.js";

export async function registerAutoResizer() {
	emitter.onAction('files.upload',  async (meta: Record<string, any>, context: EventContext) => {
		const fileKey = meta['key'] as string
		const assetsService = new AssetsService({knex: context.database, accountability: context.accountability, schema: context.schema!!})

		await assetsService.uploadThumbnails(fileKey)
	})
}
