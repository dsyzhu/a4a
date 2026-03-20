# YQL program example

PROGRAM (
    @namespace  string = '',
    @contextIds string,
    @sortBy     string = 'popular',
    @index      string = '',
    @type       string = '',
    @count      int32  = 3,
    @region     string = 'US',
    @lang       string = 'en-US',
    @sentiment  boolean = false
);

FROM CanvassUDF IMPORT concat;
FROM CanvassUDF IMPORT parseCsv;
FROM CanvassUDF IMPORT extractCanvassMesssageListItem;

CREATE TEMPORARY TABLE canvassMessageListTable AS (
    SELECT * FROM canvass.batch.message(@namespace, parseCsv(@contextIds), @index, @sortBy, parseCsv(@type), @count, @region, @lang, @sentiment)
);

CREATE TEMPORARY TABLE canvassMessageItemsTable AS (
    SELECT canvassMessageItems FROM canvassMessageListTable | extractCanvassMesssageListItem
);

CREATE TEMPORARY TABLE messagesSentimentAndCountQueryTable AS (
    SELECT contextId, count, canvassSentimentTypes FROM canvassMessageListTable
);

CREATE TEMPORARY TABLE messagesQueryTable AS (
    SELECT
        msg.contextId,
        msg.messageId,
        msg.msg,
        msg.score,
        msg.stats,
        msg.sentiment,
        index
    FROM canvassMessageItemsTable
);

CREATE TEMPORARY TABLE userProfilesTable AS (
    SELECT
        guid,
        nickname,
        image
    FROM yqlp_canvass_common.userprofile
    WHERE guid in (SELECT msg.meta.author.guid from messagesQueryTable)
);

CREATE TEMPORARY TABLE usersInfoTable AS (
    SELECT
        guid,
        userCategory
    FROM canvass.user.info
    WHERE guid in (SELECT msg.meta.author.guid from messagesQueryTable)
);

CREATE TEMPORARY TABLE messagesBatchTable AS (
    SELECT
        cm.contextId,
        cm.messageId,
        cm.msg.namespace,
        {
            "type" : cm.msg.meta.type,
            "author" : {
                "guid" : cm.msg.meta.author.guid,
                "userType" : cm.msg.meta.author.userType,
                "nickname" : up.nickname,
                "image" : up.image,
                "userCategory" : ui.userCategory
            },
            "createdAt" : cm.msg.meta.createdAt,
            "updatedAt" : cm.msg.meta.updatedAt,
            "contextInfo" : cm.msg.meta.contextInfo,
            "scoreAlgo" : cm.score.scoreAlgo,
            "visibility" : cm.msg.meta.visibility,
            "sentimentLabel": cm.sentiment.type,
            "locale": cm.msg.meta.locale
        } as meta,
        cm.msg.details,
        cm.msg.tags,
        {
            "upVoteCount": cm.stats.upVoteCount,
            "downVoteCount": cm.stats.downVoteCount,
            "abuseVoteCount": cm.stats.abuseVoteCount,
            "replyCount": cm.stats.replyCount
        } as reactionStats,
        cm.index
    FROM messagesQueryTable AS cm
    LEFT JOIN userProfilesTable AS up ON up.guid = cm.msg.meta.author.guid
    LEFT JOIN usersInfoTable AS ui ON ui.guid = cm.msg.meta.author.guid | GroupBy.usingField("contextId")
);

SELECT
    bcm.contextId as contextId,
    bcm.count as total,
    bcm.canvassSentimentTypes as sentiments,
    bm.rows as messages
FROM messagesSentimentAndCountQueryTable AS bcm
LEFT JOIN messagesBatchTable AS bm ON bcm.contextId = bm.key
OUTPUT AS canvassMessages;
