
import hashlib
import time
import urllib


class FandangoApiManager(object):

    def __init__(self):
        self.FandangoApiManager = []

    def Sha256Encode(self, stringToEncode):
        s = hashlib.sha256()
        s.update(stringToEncode)
        result = s.hexdigest()

        return result

    def BuildAuthorizationParameters(self, apiKey, sharedSecret):
        paramsToEncode = "{0}{1}{2}".format(apiKey,
                                            sharedSecret,
                                            int(time.time()))
        encodedParams = self.Sha256Encode(paramsToEncode)
        result = "apikey={0}&sig={1}".format(apiKey, encodedParams)
        return result

    def GetResponse(self, parameters):

        baseUri = "http://api.fandango.com"
        apiVersion = "1"

        apiKey = "q2t5epnc82hfrts8jbcmf4s3"
        sharedSecret = "7rNXGVpCGU"

        authParameters = self.BuildAuthorizationParameters(apiKey,
                                                           sharedSecret)
        requestUri = "{0}/v{1}/?{2}&{3}".format(baseUri,
                                                apiVersion,
                                                parameters,
                                                authParameters)

        response = urllib.urlopen(requestUri)
        result = response.read()
        return result


def main():
    api = FandangoApiManager()
    zipCode = "94105"
    parameters = "op=theatersbypostalcodesearch&postalcode={0}".format(zipCode)
    responseFromServer = api.GetResponse(parameters)
    print responseFromServer
    # process responseFromServer...


if __name__ == "__main__":
    main()
