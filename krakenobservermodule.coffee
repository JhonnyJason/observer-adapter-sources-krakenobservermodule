krakenobservermodule = {name: "krakenobservermodule"}
############################################################
#region printLogFunctions
log = (arg) ->
    if allModules.debugmodule.modulesToDebug["krakenobservermodule"]?  then console.log "[krakenobservermodule]: " + arg
    return
ostr = (obj) -> JSON.stringify(obj, null, 4)
olog = (obj) -> log "\n" + ostr(obj)
print = (arg) -> console.log(arg)
#endregion

############################################################
KrakenClient = require('kraken-api')
data = null

############################################################
#region internalProperties
kraken = null

############################################################
heartbeatMS = 0
heartBeatTimerId = 0

############################################################
usedAssetPairs = null
usedAssets = null

############################################################
latestBalances = null
latestOrders = null
latestClosedOrders = null
latestTicker = null

#endregion

############################################################
krakenobservermodule.initialize = () ->
    log "krakenobservermodule.initialize"
    data = allModules.datahandlermodule
    c = allModules.configmodule
    
    kraken       = new KrakenClient(c.apiKey, c.secret)
    
    heartbeatMS = c.observerHeartbeatM * 60 * 1000

    usedAssetPairs = allModules.krakentranslationmodule.relevantAssetPairs
    usedAssets = allModules.krakentranslationmodule.relevantAssets
    return

############################################################
#region internalFunctions
heartbeat = ->
    log " > heartbeat"
    await getLatestBalance()
    await getLatestMarketData()
    await getLatestOrders()
    digestCurrentData()
    return

############################################################
#region heartbeatFunctions
getLatestOrders = ->
    try
        response = await kraken.api("OpenOrders")
        latestOrders = response.result.open
        response = await kraken.api("ClosedOrders")
        latestClosedOrders = response.result.closed
    catch err
        log "Error on getLatestOrders"
        log err           
    return

getLatestBalance = ->
    try
        response = await kraken.api('Balance')
        latestBalances = response.result
        print ostr latestBalances
    catch err
        log "Error on getLatestBalance"
        log err   
    return

getLatestMarketData = ->
    # log "getCurrentMarketData"
    ## at maximum 1 request every 4 seconds
    try
        pair = usedAssetPairs.map( (el) -> el.krakenName )
        pair = pair.join(",")
        response = await kraken.api("Ticker", {pair: pair})
        latestTicker = response.result 
    catch err
        log "Error on getLatestMarketData"
        log err   
    return

#endregion

############################################################
#region helperFunctions
digestCurrentData = ->
    try
        digestRelevantBalances()
        digestRelevantOrders()
        digestRelevantPrices()
    catch err 
        log "Error in digestCurrentData!"
        log err
    return

############################################################
digestRelevantBalances = ->
    for asset in usedAssets
        if !latestBalances? then balance = "0"
        else balance = latestBalances[asset.krakenName]
        if !balance? then balance = "0"
        data.setAssetBalance(asset.ourName, parseFloat(balance))
    return

digestRelevantOrders = ->
    # olog latestOrders
    for pair in usedAssetPairs
        buyOrders = getBuyOrdersFor(pair)
        sellOrders = getSellOrdersFor(pair)
        cancelledOrders = getCancelledOrdersFor(pair)
        filledOrders = getFilledOrdersFor(pair)
        
        data.setBuyStack(pair.ourName, buyOrders)
        data.setSellStack(pair.ourName, sellOrders)
        data.setCancelledStack(pair.ourName, cancelledOrders)
        data.setFilledStack(pair.ourName, filledOrders)
    return

digestRelevantPrices = ->
    for pair in usedAssetPairs
        krakenTicker = latestTicker[pair.krakenName]
        ticker = 
            askPrice: parseFloat(krakenTicker.a[0])
            bidPrice: parseFloat(krakenTicker.b[0])
            closingPrice: parseFloat(krakenTicker.c[0])
        data.setTicker(pair.ourName, ticker)
    return

############################################################
getCancelledOrdersFor = (pair) ->
    ourOrders = []
    for id,order of latestClosedOrders when order.descr.ordertype == "limit"  and order.descr.pair == pair.orderName and order.status == "canceled"
        o = {}
        o.id = id
        o.time = order.closetm
        if order.descr.type == "buy" then o.type = "buy"
        if order.descr.type == "sell" then o.type = "sell"
        o.price = parseFloat(order.descr.price)
        o.volume = parseFloat(order.vol)
        ourOrders.push o 
    return ourOrders

getFilledOrdersFor = (pair) ->
    ourOrders = []
    for id,order of latestClosedOrders when order.descr.ordertype == "limit"  and order.descr.pair == pair.orderName and order.status == "closed"
        o = {}
        o.id = id
        o.time = parseInt(order.closetm*1000)
        if order.descr.type == "buy" then o.type = "buy"
        if order.descr.type == "sell" then o.type = "sell"
        o.price = parseFloat(order.descr.price)
        o.volume = parseFloat(order.vol)
        ourOrders.push o 
    return ourOrders

getBuyOrdersFor = (pair) ->
    ourOrders = []
    for id,order of latestOrders when order.descr.ordertype == "limit" and order.descr.type == "buy" and order.descr.pair == pair.orderName
        o = {}
        o.id = id
        o.type = "buy"
        o.price = parseFloat(order.descr.price)
        o.volume = parseFloat(order.vol)
        ourOrders.push o 
    return ourOrders

getSellOrdersFor = (pair) ->
    ourOrders = []
    for id,order of latestOrders when order.descr.ordertype == "limit" and order.descr.type == "sell" and order.descr.pair == pair.orderName
        o = {}
        o.id = id
        o.type = "sell"
        o.price = parseFloat(order.descr.price)
        o.volume = parseFloat(order.vol)
        ourOrders.push o 
    return ourOrders

#endregion

############################################################
getAllAssetPairs = ->
    try
        response = await kraken.api("AssetPairs")
        olog Object.keys(response.result)
    catch err
        log "Error on getAllAssetPairs"
        log err           
    return

#endregion

############################################################
krakenobservermodule.startObservation = ->
    log "krakenobservermodule.startObservation"
    # getAllAssetPairs()
    heartbeat()
    heartBeatTimerId = setInterval(heartbeat, heartbeatMS)
    return

#endregion

module.exports = krakenobservermodule