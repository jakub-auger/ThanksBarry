from alpha_vantage.timeseries import TimeSeries
import yfinance as yf
from datetime import datetime

# 
# Thanks, Barry!
#
# A simple robo-broker that'll tell you when interesting stuff happens
#
# He's too dumb to tell you what to buy, but he watches the market like a hawk
# and will tell you when something you own has shot up and when sentiment changes and you should probably sell
#
#
#
##
##
##
#
def askBarry(fbuySell, fticker, fmonitorDate, ftargetPrice, favKey='', fdestinationTimeZone='Australia/Sydney', fmonitorThreshold=0.05, fnoiseThreshold=.025):

    # monitor a stock at a threshold
    # buy: notify me when it bottoms out and starts heading up
    # sell: notify me when it tops out and starts heading down

    # how it works: dumb rolling min/max and a noise threshold % so it doesnt notify until the transition is > the %

    # set % threshold for monitoring and a 'noise' band remove unnecessary notifications
    # the monitor threshold tells us when the stock moves from our set price (default of 5%)
    # the noise band waits until we're 2.5% (default) from the min/max before flagging it as the turning point
    monitorThreshold = 1-(fmonitorThreshold if fbuySell == 'buy' else -fmonitorThreshold if fbuySell == 'sell' else 0)
    noiseThreshold = 1+(fnoiseThreshold if fbuySell == 'buy' else -fnoiseThreshold if fbuySell == 'sell' else 0)

    act = 'none'
    now = datetime.now().strftime("%b %d %Y %H:%M:%S")

    # the funciton returns a list of all the variables and outputs of the process so we can do other stuff with it if required
    outputList = {'now': now,'ticker':fticker.upper(), 'monitorDate':fmonitorDate, 'targetPrice':ftargetPrice, 'status':"query"}
    # create a generic message that we can show by default that explains what's happened 
    outputData = ('{3}\n{0} target set at ${2:,.2f} commencing from {1}').format(fticker.upper(),fmonitorDate,ftargetPrice,now)


    # a mapping of the conformed attributes to the corresponding attributes which come from the ticker price external api (alpha vantage/yahoo finance)
    dataMapping = {
                "Datetime": "Datetime",
                "Open": "Open",
                "Low": "Low",
                "High": "High",
                "Close": "Close",
                "Volume": "Volume"
        }

    # check inputs. only process if its a buy or a sell
    if fbuySell == 'buy' : 
        whichCol = dataMapping['Low']

    elif fbuySell == 'sell' :
        whichCol = dataMapping['High']

    else :
        return ('error',outputList,'ERROR - bad action. Check config\n'+outputData)


    #get the intraday data from alpha vantage. It comes back as a dataframe in format date, open, low, high, close, volume
    #ts = TimeSeries(key=favKey, output_format='pandas')
    #data, meta_data = ts.get_intraday(symbol=fticker,interval='1min', outputsize='full')


    # get price data from yahoo
    data = yf.download(  # or pdr.get_data_yahoo(...
        # tickers list or string as well
        tickers = fticker,

        # use "period" instead of start/end
        # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
        # (optional, default is '1mo')
        period = "5d",

        # fetch data by interval (including intraday if period < 60 days)
        # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
        # (optional, default is '1d')
        interval = "1m",

        # group by ticker (to access via data['SPY'])
        # (optional, default is 'column')
        group_by = 'column',

        # adjust all OHLC automatically
        # (optional, default is False)
        auto_adjust = True,

        # download pre/post regular market hours data
        # (optional, default is False)
        prepost = True,

        # use threads for mass downloading? (True/False/Integer)
        # (optional, default is True)
        threads = True,

        # proxy URL scheme use use when downloading?
        # (optional, default is None)
        proxy = None
    )

    # no data/bad ticker. exit
    if data.empty:
        print ('empty')
        return ('error',outputList,'ERROR - bad ticker. Check config\n'+outputData)

    # change it to be in ascending order (it comes in descending). I want to test the data in the same 'direction' as it arrives
    data.sort_index(inplace=True,ascending=True)

    #convert the date index into a real column; i need to do date comparisons
    data.reset_index(level=0, inplace=True)
    #print('my raw data')
    #pprint(data)

    # remove the columns we dont need
    data.drop([dataMapping['Open'],dataMapping['Volume']],axis=1,inplace=True)

    # alpha vantage comes in EST datetime. Convert it to the ASX timezone (sydney)
    #data['date']=(data['datetime'].dt.tz_localize('US/Eastern').dt.tz_convert(fdestinationTimeZone) )

    # Remove data prior to my buy date. Don't need it
    data = data.loc[data[dataMapping['Datetime']]>= fmonitorDate]

    # add a rolling max. I want to know how high it gets and test how far it drops from that high
    if fbuySell == 'buy' : 
        data['rollingAgg'] = data[whichCol].cummin(axis=0)

    if fbuySell == 'sell' :
        data['rollingAgg'] = data[whichCol].cummax(axis=0)

    # trigger notifications only after the stock reaches a certain amount compared to the input price. This prevents excessive early notifications. Eg i only care about the stock after
    # its gone up x%
    if fbuySell == 'buy' : 
        monitor = data.loc[data[whichCol] <= (ftargetPrice*monitorThreshold)].head(1)

    if fbuySell == 'sell' :
        monitor = data.loc[data[whichCol] >= (ftargetPrice*monitorThreshold)].head(1)
    
    # monitor threshold reached. Add the corresponding attributes to the output list and append relevant text to the output string
    if not monitor.empty :
        act = 'watch'
    
        monitorStartDate = monitor[dataMapping['Datetime']].tolist()[0]
        outputList["monitorStartDate"]=monitorStartDate
        
        outputList["monitorStartPrice"]=monitor[whichCol].tolist()[0]
        
        outputList["status"]='monitor'
        
        outputData = outputData + (' reached the monitor threshold of ${0:,.2f} on {1} (${2:,.2f}).\n').format(ftargetPrice*monitorThreshold,
                                                                                                             outputList["monitorStartDate"], outputList["monitorStartPrice"])

        #prune data so it only holds info after the monitor date (less filters needed for subsequent testing)
        data = data[data[dataMapping['Datetime']]>= monitorStartDate]

        # Check if the sentiment has changed and the stock price has changed direction. The noise band ignores minor wiggles
        if fbuySell == 'buy' : 
            final = data.loc[data[whichCol] >= (data['rollingAgg'] * noiseThreshold)].head(1)

        if fbuySell == 'sell' :
            final = data.loc[data[whichCol] <= (data['rollingAgg'] * noiseThreshold)].head(1)

        # price has changed direction. Set add relevant attributes to the output list and append the generic text to the output string
        if not final.empty :

            act = 'act'
                        
            # pull scalars out of the data frame. Need it for the notifications
            actPrice = final['rollingAgg'].tolist()[0]

            ##print('max price')
            
            dateMonitorReached = (data.loc[(data[whichCol] == actPrice) & (data[dataMapping['Datetime']] >= monitorStartDate)].head(1))[dataMapping['Datetime']].tolist()[0]
            
            outputList["actPrice"] = actPrice
            outputList["dateMonitorReached"] = dateMonitorReached
            outputList["actTriggerDate"] = final[dataMapping['Datetime']].tolist()[0]
            outputList["actTriggerPrice"] = final[whichCol].tolist()[0]
            outputList["status"]=fbuySell
            
            outputData = outputData + ('{5} of ${0:,.2f} reached on {1}.\n'
                                       '{6} signal threshold of ${2:,.2f} reached on {3} (${4:,.2f}).\n').format(outputList["actPrice"],
                                                               outputList["dateMonitorReached"], outputList["actPrice"]*noiseThreshold, outputList["actTriggerDate"], outputList["actTriggerPrice"]
                                                                                                                 ,whichCol,fbuySell) 


        else :
            outputData = outputData + ('No signal to {0}\n').format(fbuySell)

    else :
        outputData = outputData + ('\nmonitor threshold ${0:,.2f} not reached\n').format(ftargetPrice*monitorThreshold)

    #Add the current position and date just so we know what it is
    outputList["currentPrice"] = data.tail(1)[dataMapping['Close']].tolist()[0]
    outputList["currentDate"] = data.tail(1)[dataMapping['Datetime']].tolist()[0]


    outputData = outputData + ('{0} is currently at ${1:,.2f} on {2}\n').format(outputList["ticker"],outputList["currentPrice"],outputList["currentDate"])
    
    return(act,outputList,outputData)
