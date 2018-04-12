from pyOutlook import OutlookAccount

account_one=OutlookAccount('Nowornever@357')

inbox=account_one.inbox()
print(inbox[0].body)