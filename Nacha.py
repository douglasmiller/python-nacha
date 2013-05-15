from datetime import datetime, timedelta
from math import floor
import string

NACHA_EOL = "\r\n"

class NachaFile:

    def __init__(self, modifier, destination, destinationName, origin, originName):
        #Mark as not finalized
        self.finalized = False;

        #Initialize the batches list
        self.batches = []

        #Initialize the nineFill
        self.nineFill = ""

        #Create the File Header and File Control records
        self.fileHeader = NachaFileHeader(modifier)
        self.fileControl = NachaFileControl()

        #Set supplied values
        self.fileHeader.setValue("destination", destination)
        self.fileHeader.setValue("origin", origin)
        self.fileHeader.setValue("destinationName", destinationName)
        self.fileHeader.setValue("originName", originName)

    def writeToFile(self, fileName):
        if self.finalized:
            file = open(fileName, "w")
            file.write(self.toString())
            file.close()
        else:
            raise NachaError("The file cannot be written until it is finalized.")

    def addBatch(self, batch):
        if not self.finalized:
            batch.finalize(len(self.batches) + 1)
            self.batches.append(batch)
        else:
            raise NachaError("Batches cannot be added after the file is finalized.")

    def finalize(self):
        if self.finalized:
            return

        #Set the total Batch Count
        self.fileControl.setValue("batchCount", len(self.batches))

        #Get the totals from all the batches
        entryCount = 0
        entryHash = 0
        debitAmount = 0
        creditAmount = 0
        for batch in self.batches:
            entryCount += int(batch.batchControl.getValue("entryCount"))
            entryHash += int(batch.batchControl.getValue("entryHash"))
            debitAmount += int(batch.batchControl.getValue("debitAmount"))
            creditAmount += int(batch.batchControl.getValue("creditAmount"))
        #Set the entryCount
        self.fileControl.setValue("entryCount", entryCount)
        #Obtain the rightmost 10 digits of the hash
        entryHash = entryHash % 10000000000
        self.fileControl.setValue("entryHash", entryHash)
        #Set the Total Debit Entry Amount
        self.fileControl.setValue("debitAmount", debitAmount)
        #Set the Total Credit Entry Amount
        self.fileControl.setValue("creditAmount", creditAmount)

        #Calculate and set the Block Count
        #There are 2 records for the file (File Header and File Control)
        #There are 2 records for each batch (Batch Header and Batch Control)
        blockingFactor = int(self.fileHeader.getValue("blockingFactor"))
        recordCount = 2 + (len(self.batches) * 2) + entryCount
        blockCount = int(floor(recordCount / blockingFactor))
        blockMod = recordCount % blockingFactor
        if blockMod != 0:
            blockCount += 1
            self.nineFill = NACHA_EOL.join(["9" * 94] * (blockingFactor - blockMod))
        self.fileControl.setValue("blockCount", blockCount)

        self.fileHeader.lock()
        self.fileControl.lock()
        #Mark as finalized
        self.finalized = True

    def toString(self):
        header = str(self.fileHeader.data)
        control = str(self.fileControl.data)
        batches = NACHA_EOL.join(batch.toString() for batch in self.batches)
        return NACHA_EOL.join([header, batches, control, str(self.nineFill)])


class NachaBatch:

    def __init__(self, serviceCode, classCode, companyName, description, companyId, odfiId, effectiveDate):
        #Mark as not finalized
        self.finalized = False;

        #Initialize the entries list
        self.entries = []

        #Create Header and Control records
        self.batchHeader = NachaBatchHeader()
        self.batchControl = NachaBatchControl()

        #Set passed values 
        self.setDualField("serviceCode", serviceCode)
        self.batchHeader.setValue("entryClassCode", classCode)
        self.batchHeader.setValue("companyName", companyName)
        self.batchHeader.setValue("entryDescription", description)
        today = datetime.today()
        self.batchHeader.setValue("descriptiveDate", today.strftime("%b %y"))
        #add a day until the effectiveDate is not Saturday(5) of Sunday(6)
        while effectiveDate.weekday() in [5,6]:
            effectiveDate += timedelta(days=1)
        #TODO: ensure that this is a valid banking day (not a holiday?)
        self.batchHeader.setValue("entryDate", effectiveDate.strftime("%y%m%d"))
        self.setDualField("companyId", companyId)
        self.setDualField("odfiId", odfiId)

    def addEntry(self, entry):
        if not self.finalized:
            entry.setValue("odfiId", self.batchHeader.getValue("odfiId"))
            entry.setValue("sequenceNumber", len(self.entries) + 1)
            entry.lock()
            self.entries.append(entry)
        else:
            raise NachaError("Entries cannot be added after the batch has been finalized.")
    
    def setDualField(self, fieldName, value):
        self.batchHeader.setValue(fieldName, value)
        self.batchControl.setValue(fieldName, value)

    def finalize(self, batchNumber):
        if self.finalized:
            return

        #Set the batch number
        self.setDualField("batchNumber", batchNumber)

        #Set the number of entries
        self.batchControl.setValue("entryCount", len(self.entries))

        #Calculate and set the Entry Hash
        entryHash = 0
        debitAmount = 0
        creditAmount = 0
        serviceCode = self.batchHeader.getValue("serviceCode")
        for entry in self.entries:
            entryHash += int(entry.getValue("rdfiId"))
            #Currently we do not support debits, but this is here anyway
            if serviceCode in (NachaBatchHeader.DEBITS_ONLY_SERVICE, NachaBatchHeader.MIXED_SERVICE):
                #TODO: Ensure that this is a debit
                debitAmount += int(entry.getValue("amount"))
            if serviceCode in (NachaBatchHeader.CREDITS_ONLY_SERVICE, NachaBatchHeader.MIXED_SERVICE):
                #TODO: Ensure that this is a credit
                creditAmount += int(entry.getValue("amount"))
        #Obtain the rightmost 10 digits of the entryHash
        entryHash = entryHash % 10000000000
        self.batchControl.setValue("entryHash", entryHash)
        #Summate the entries that are credits
        self.batchControl.setValue("creditAmount", creditAmount)
        #Summate the entries that are debits
        self.batchControl.setValue("debitAmount", debitAmount)

        self.batchHeader.lock()
        self.batchControl.lock()
        #Mark this batch as finalized
        self.finalized = True

    def toString(self):
        header = str(self.batchHeader.data)
        control = str(self.batchControl.data)
        entries = NACHA_EOL.join(entry.toString() for entry in self.entries)
        return NACHA_EOL.join([header, entries, control])


class NachaRecord:
    
    def __init__(self):
        self.locked = False
        self.fields = {}
        self.data = bytearray(b" " * 94)

    def lock(self):
        self.locked = True

    def setValue(self, fieldName, value):
        if self.locked:
            raise NachaError("Cannot set values on a locked record.")
        if fieldName in self.fields:
            field = self.fields[fieldName]
            start = field.start
            end = field.end
            if field.type is not None:
                #Pad the value according to the definition of the field
                value = getattr(string, NachaField.PADDING_FUNCTION[field.type])(
                        str(value), 
                        end - start, 
                        NachaField.PADDING_CHAR[field.type]
                        ) 
            #Insert the value into the data string.
            #Do not exceed the allowed length of the field
            self.data[start:end] = str(value)[0:end - start]
        else:
            raise NachaError(fieldName + " isn't defined in " + self.__class__.__name__ + ".")

    def getValue(self, fieldName):
        if fieldName in self.fields:
            field = self.fields[fieldName]
            start = field.start
            end = field.end
            return str(self.data[start:end])
        else:
            raise NachaError(fieldName + " isn't defined in " + self.__class__.__name__ + ".")
            return None

    def toString(self):
        return str(self.data) 
            


class NachaField:

    NUMERIC = 0
    ALPHAMERIC = 1
    ROUTING = 2
    PADDING_FUNCTION = ("rjust", "ljust", "rjust")
    PADDING_CHAR = ("0", " ", " ")
    
    def __init__(self, name, start, end):
        self.name = name
        self.start = start - 1
        self.end = end
        self.type = None

    def setType(self, type):
        self.type = type


class NachaFileHeader(NachaRecord):

    def __init__(self, fileIdModifier):
        NachaRecord.__init__(self)

        #Define the Fields
        self.fields["recordType"] = NachaField("Record Type", 1, 1)
        self.fields["priorityCode"] = NachaField("Priority Code", 2, 3)
        self.fields["destination"] = NachaField("Immediate Destination", 4, 13)
        self.fields["origin"] = NachaField("Immediate Origin", 14, 23)
        self.fields["creationDate"] = NachaField("File Creation Date", 24, 29)
        self.fields["creationTime"] = NachaField("File Creation Time", 30, 33)
        self.fields["fileIdModifier"] = NachaField("File ID Modifier", 34, 34)
        self.fields["recordSize"] = NachaField("Record Size", 35, 37)
        self.fields["blockingFactor"] = NachaField("Blocking Factor", 38, 39)
        self.fields["formatCode"] = NachaField("Format Code", 40, 40)
        self.fields["destinationName"] = NachaField("Immediate Destination Name", 41, 63)
        self.fields["originName"] = NachaField("Immediate Origin Name", 64, 86)
        self.fields["refCode"] = NachaField("Reference Code", 87, 94)

        #Define the Field Types where necessary
        self.fields["priorityCode"].setType(NachaField.NUMERIC)
        self.fields["destination"].setType(NachaField.ROUTING)
        self.fields["origin"].setType(NachaField.ROUTING)
        self.fields["destinationName"].setType(NachaField.ALPHAMERIC)
        self.fields["originName"].setType(NachaField.ALPHAMERIC)
        self.fields["refCode"].setType(NachaField.ALPHAMERIC)

        #Set the default values
        self.setValue("recordType", "1")
        self.setValue("priorityCode", "01")
        self.setValue("creationDate", datetime.today().strftime("%y%m%d"))
        self.setValue("creationTime", datetime.today().strftime("%H%M"))
        self.setValue("recordSize", "094")
        self.setValue("blockingFactor", "10")
        self.setValue("formatCode", "1")
        self.setValue("refCode", "C4 TECH")

        #Set the supplied values
        self.setValue("fileIdModifier", fileIdModifier)


class NachaFileControl(NachaRecord):

    def __init__(self):
        NachaRecord.__init__(self)

        #Define the Fields
        self.fields["recordType"] = NachaField("Record Type", 1, 1)
        self.fields["batchCount"] = NachaField("Batch Count", 2, 7)
        self.fields["blockCount"] = NachaField("Block Count", 8, 13)
        self.fields["entryCount"] = NachaField("Entry Count", 14, 21)
        self.fields["entryHash"] = NachaField("Entry Hash", 22, 31)
        self.fields["debitAmount"] = NachaField("Total Debit Amount", 32, 43)
        self.fields["creditAmount"] = NachaField("Total Credit Amount", 44, 55)
        self.fields["reserved"] = NachaField("Reserved", 56, 94)

        #Define the Field Types where necessary
        self.fields["batchCount"].setType(NachaField.NUMERIC)
        self.fields["blockCount"].setType(NachaField.NUMERIC)
        self.fields["entryCount"].setType(NachaField.NUMERIC)
        self.fields["entryHash"].setType(NachaField.NUMERIC)
        self.fields["debitAmount"].setType(NachaField.NUMERIC)
        self.fields["creditAmount"].setType(NachaField.NUMERIC)

        #Set the default values
        self.setValue("recordType", "9")


class NachaBatchHeader(NachaRecord):

    PPD_ENTRY = "PPD"

    EIN = "1"
    DUNS = "3"

    MIXED_SERVICE = "200"
    CREDITS_ONLY_SERVICE = "220"
    DEBITS_ONLY_SERVICE = "225"
    ADVICES_SERVICE = "280"

    def __init__(self):
        NachaRecord.__init__(self)

        #Define the Fields
        self.fields["recordType"] = NachaField("Record Type", 1, 1)
        self.fields["serviceCode"] = NachaField("Service Class Code", 2, 4)
        self.fields["companyName"] = NachaField("Company Name", 5, 20)
        self.fields["companyData"] = NachaField("Company Discretionary Date", 21, 40)
        self.fields["companyId"] = NachaField("Company Identification", 41, 50)
        self.fields["entryClassCode"] = NachaField("Standard Entry Class Code", 51, 53)
        self.fields["entryDescription"] = NachaField("Company Entry Description", 54, 63)
        self.fields["descriptiveDate"] = NachaField("Company Descriptive Date", 64, 69)
        self.fields["entryDate"] = NachaField("Effective Entry Date", 70, 75)
        self.fields["settlementDate"] = NachaField("Settlement Date", 76, 78)
        self.fields["originatorCode"] = NachaField("Originator Status Code", 79, 79)
        self.fields["odfiId"] = NachaField("Originating DFI Identification", 80, 87)
        self.fields["batchNumber"] = NachaField("Batch Number", 88, 94)

        #Define the Field Types where necessary
        self.fields["serviceCode"].setType(NachaField.NUMERIC)
        self.fields["companyName"].setType(NachaField.ALPHAMERIC)
        self.fields["companyData"].setType(NachaField.ALPHAMERIC)
        self.fields["companyId"].setType(NachaField.ALPHAMERIC)
        self.fields["entryClassCode"].setType(NachaField.ALPHAMERIC)
        self.fields["entryDescription"].setType(NachaField.ALPHAMERIC)
        self.fields["descriptiveDate"].setType(NachaField.ALPHAMERIC)
        self.fields["settlementDate"].setType(NachaField.NUMERIC)
        self.fields["odfiId"].setType(NachaField.ALPHAMERIC)
        self.fields["batchNumber"].setType(NachaField.NUMERIC)

        #Set the default values
        self.setValue("recordType", "5")


class NachaBatchControl(NachaRecord):

    def __init__(self):
        NachaRecord.__init__(self)

        #Define the Fields
        self.fields["recordType"] = NachaField("Record Type", 1, 1)
        self.fields["serviceCode"] = NachaField("Service Class Code", 2, 4)
        self.fields["entryCount"] = NachaField("Entry/Addenda Count", 5, 10)
        self.fields["entryHash"] = NachaField("Entry Hash", 11, 20)
        self.fields["debitAmount"] = NachaField("Total Debit Entry Dollar Amount", 21, 32)
        self.fields["creditAmount"] = NachaField("Total Credit Entry Dollar Amount", 33, 44)
        self.fields["companyId"] = NachaField("Company Identification", 45, 54)
        self.fields["authCode"] = NachaField("Message Authentication Code", 55, 73)
        self.fields["reserved"] = NachaField("Reserved", 74, 79)
        self.fields["odfiId"] = NachaField("Originating DFI Identification", 80, 87)
        self.fields["batchNumber"] = NachaField("Batch Number", 88, 94)

        #Define the Field Types where necessary
        self.fields["serviceCode"].setType(NachaField.NUMERIC)
        self.fields["entryCount"].setType(NachaField.NUMERIC)
        self.fields["entryHash"].setType(NachaField.NUMERIC)
        self.fields["debitAmount"].setType(NachaField.NUMERIC)
        self.fields["creditAmount"].setType(NachaField.NUMERIC)
        self.fields["companyId"].setType(NachaField.ALPHAMERIC)
        self.fields["authCode"].setType(NachaField.ALPHAMERIC)
        self.fields["batchNumber"].setType(NachaField.NUMERIC)

        #Set the default values
        self.setValue("recordType", "8")


class NachaEntry(NachaRecord):

    SAVINGS_CREDIT = "32"
    CHECKING_CREDIT = "22"

    CHECK_DIGIT_WEIGHTS = (3, 7, 1, 3, 7, 1, 3, 7)

    def __init__(self, transCode, rdfiId, dfiActNum, amount, id, name):
        NachaRecord.__init__(self)

        #Define the Fields
        self.fields["recordType"] = NachaField("Record Type", 1, 1)
        self.fields["transactionCode"] = NachaField("Transaction Code", 2, 3)
        self.fields["rdfiId"] = NachaField("Receiving DFI Identification", 4, 11)
        self.fields["checkDigit"] = NachaField("Check Digit", 12, 12)
        self.fields["dfiAccountNumber"] = NachaField("DFI Account Number", 13, 29)
        self.fields["amount"] = NachaField("Amonut", 30, 39)
        self.fields["idNumber"] = NachaField("Individual Identification Number", 40, 54)
        self.fields["name"] = NachaField("Individual Name", 55, 76)
        self.fields["data"] = NachaField("Discretionary Data", 77, 78)
        self.fields["addenda"] = NachaField("Addenda Record Indicator", 79, 79)
        self.fields["traceNumber"] = NachaField("Trace Number", 80, 94)
        #odfiId and sequenceNumber are components of the traceNumber
        self.fields["odfiId"] = NachaField("Trace Number - Originating DFI Identification", 80, 88)
        self.fields["sequenceNumber"] = NachaField("Trace Number - Sequence Number", 88, 94)

        #Define the Field Types where necessary
        self.fields["transactionCode"].setType(NachaField.NUMERIC)
        self.fields["checkDigit"].setType(NachaField.NUMERIC)
        self.fields["idNumber"].setType(NachaField.ALPHAMERIC)
        self.fields["dfiAccountNumber"].setType(NachaField.ALPHAMERIC)
        self.fields["amount"].setType(NachaField.NUMERIC)
        self.fields["name"].setType(NachaField.ALPHAMERIC)
        self.fields["data"].setType(NachaField.ALPHAMERIC)
        self.fields["addenda"].setType(NachaField.NUMERIC)
        self.fields["traceNumber"].setType(NachaField.NUMERIC)
        self.fields["sequenceNumber"].setType(NachaField.NUMERIC)

        #Set the default values
        self.setValue("recordType", "6")
        #This model does not support Addenda Records
        self.setValue("addenda", "0")

        #Set the supplied values
        self.setValue("transactionCode", transCode)
        self.setValue("rdfiId", rdfiId)
        self.setValue("dfiAccountNumber", dfiActNum)
        self.setValue("amount", amount)
        self.setValue("idNumber", id)
        self.setValue("name", name)

    def setValue(self, fieldName, value):
        NachaRecord.setValue(self, fieldName, value)
        if fieldName == "rdfiId":
            NachaRecord.setValue(self, "checkDigit", self.calculateCheckDigit())

    def calculateCheckDigit(self):
        rdfiId = self.getValue("rdfiId")
        rdfiList = [int(char) for char in list(rdfiId)]
        total = sum([x*y for x, y in zip(self.CHECK_DIGIT_WEIGHTS, rdfiList)])
        nextTen = total + (10 - total % 10)
        return nextTen - total


class NachaError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)
