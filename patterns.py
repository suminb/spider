URL_PATTERNS = (

	# Microsoft (all topics)
	r"http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_M/forumview\?bn=12004[\/\w %=;&\.\-\+\?]*\/?",

	# Microsoft (single topic)
	r"http://messages.finance.yahoo.com/Business_%26_Finance/Investments/Stocks_%28A_to_Z%29/Stocks_M/threadview\?[\/\w %=;&\.\-\+\?]*bn=12004[\/\w %=;&\.\-\+\?]*\/?",

	# Microsoft (all messages)
	r"http://messages.finance.yahoo.com/Stocks_%28A_to_Z%29/Stocks_M/messagesview\?bn=12004[\/\w %=;&\.\-\+\?]*\/?",

    #r"http://messages.finance.yahoo.com/[A-Z][\/\w %=;&\.\-\+\?]*\/?",
    r"http://messages.finance.yahoo.com/search[\/\w %=;&\.\-\+\?]*\/?",
)