import EndPointDataFetcher
data, count = EndPointDataFetcher.make_call()
if len(data) == count:
    print("Consistent")
print(data)
