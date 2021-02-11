"""
Items of different kinds made from dictionaries
"""

import abc
import datetime
from typing import Dict

import constants
from converters import convert_with_check, MaxLenChecker, RangeChecker, EnumChecker
from trans_prefix_brand import trans_code


class BaseItem(abc.ABC):
    mpn_checker = brand_checker = MaxLenChecker(256)
    prefix_checker = MaxLenChecker(10)

    @abc.abstractmethod
    def __init__(self):
        pass

    def __repr__(self):
        return '<{} object: __dict__:{}>'.format(self.__class__.__name__, self.__dict__)


class KeystoneItem(BaseItem):
    LongDescription_checker = MaxLenChecker(5000)
    JobberPrice_checker = Cost_checker = RangeChecker(0, 999999.9999)
    ExeterQty_checker = MidWestQty_checker = SouthEastQty_checker = TexasQty_checker = PacificNWQty_checker = \
        GreatLakesQty_checker = CaliforniaQty_checker = TotalQty_checker = RangeChecker(0, 16777215)
    UPCCode_checker = MaxLenChecker(100)
    Prop65Toxicity_checker = EnumChecker(('B', 'C', 'N', 'R'))

    def __init__(self, data: Dict[str, str], *, full_parse=False) -> None:
        mpn = convert_with_check(
            value=data['PartNumber'].lstrip('="').rstrip('"'),
            output_type=str,
            checker=self.mpn_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.mpn = constants.delete_unprintable(mpn)

        brand = convert_with_check(
            value=data['VendorName'],
            output_type=str,
            checker=self.brand_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.brand = constants.delete_unprintable(brand)

        prefix = convert_with_check(
            value=data['VenCode'],
            output_type=str,
            checker=self.prefix_checker,
            replace_if_false=False,
            warn_if_false=False,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.prefix = constants.delete_unprintable(prefix)

        self.number = self.prefix + ' ' + self.mpn
        self.norm_mpn = constants.normalize_number(self.mpn)
        self.norm_brand = constants.normalize_brand(self.brand)

        if full_parse:
            self.LongDescription = convert_with_check(
                value=data['LongDescription'],
                output_type=str,
                checker=self.LongDescription_checker,
                info=data
            )
            self.JobberPrice = convert_with_check(
                value=data['JobberPrice'],
                output_type=float,
                checker=self.JobberPrice_checker,
                info=data
            )
            self.Cost = convert_with_check(
                value=data['Cost'],
                output_type=float,
                checker=self.Cost_checker,
                info=data
            )
            self.Fedexable = True if data['Fedexable'] == 'True' else False if data['Fedexable'] == 'False' else None
            self.ExeterQty = convert_with_check(
                value=data['ExeterQty'],
                output_type=int,
                checker=self.ExeterQty_checker,
                info=data
            )
            self.MidWestQty = convert_with_check(
                value=data['MidWestQty'],
                output_type=int,
                checker=self.MidWestQty_checker,
                info=data
            )
            self.SouthEastQty = convert_with_check(
                value=data['SouthEastQty'],
                output_type=int,
                checker=self.SouthEastQty_checker,
                info=data
            )
            self.TexasQty = convert_with_check(
                value=data['TexasQty'],
                output_type=int,
                checker=self.TexasQty_checker,
                info=data
            )
            self.PacificNWQty = convert_with_check(
                value=data['PacificNWQty'],
                output_type=int,
                checker=self.PacificNWQty_checker,
                info=data
            )
            self.GreatLakesQty = convert_with_check(
                value=data['GreatLakesQty'],
                output_type=int,
                checker=self.GreatLakesQty_checker,
                info=data
            )
            self.CaliforniaQty = convert_with_check(
                value=data['CaliforniaQty'],
                output_type=int,
                checker=self.CaliforniaQty_checker,
                info=data
            )
            self.TotalQty = convert_with_check(
                value=data['TotalQty'],
                output_type=int,
                checker=self.TotalQty_checker,
                info=data
            )
            self.UPCCode = convert_with_check(
                value=data['UPCCode'].lstrip('="').rstrip('"'),
                output_type=str,
                checker=self.UPCCode_checker,
                info=data
            )
            self.Prop65Toxicity = convert_with_check(
                value=data['Prop65Toxicity'],
                output_type=str,
                checker=self.Prop65Toxicity_checker,
                info=data
            )
            self.HazardousMaterial = True if data['HazardousMaterial'] else False

        super().__init__()


class MeyerItem(BaseItem):
    Meyer_SKU_checker = MaxLenChecker(266)
    Description_checker = MaxLenChecker(5000)
    Qty_008_checker = Qty_032_checker = Qty_041_checker = Qty_044_checker = Qty_053_checker = \
        Qty_062_checker = Qty_063_checker = Qty_065_checker = Qty_068_checker = Qty_069_checker = \
        Qty_070_checker = Qty_071_checker = Qty_072_checker = Qty_077_checker = Qty_093_checker = \
        Qty_094_checker = Qty_098_checker = RangeChecker(0, 16777215)
    Jobber_Price_checker = Customer_Price_checker = MAP_checker = RangeChecker(0, 999999.9999)
    UPC_checker = MaxLenChecker(100)
    Weight_checker = Height_checker = Length_checker = Width_checker = RangeChecker(0, 999999.99)
    Category_checker = Sub_Category_checker = MaxLenChecker(256)

    def __init__(self, data: Dict[str, str], *, full_parse=False, inventory=False) -> None:
        if inventory:  # Разбор inventory
            _Meyer_SKU = convert_with_check(
                value=data['Meyer SKU'],
                output_type=str,
                checker=self.Meyer_SKU_checker,
                replace_if_false=False,
                warn_if_false=True,
                replace_if_conversion_error=False,
                warn_if_conversion_error=True,
                replace_if_checker_error=False,
                warn_if_checker_error=True,
                info=data
            )
            self.Meyer_SKU = constants.delete_unprintable(_Meyer_SKU)

            self.Qty_008 = convert_with_check(
                value=data['008 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_008_checker,
                info=data
            )
            self.Qty_032 = convert_with_check(
                value=data['032 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_032_checker,
                info=data
            )
            self.Qty_041 = convert_with_check(
                value=data['041 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_041_checker,
                info=data
            )
            self.Qty_044 = convert_with_check(
                value=data['044 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_044_checker,
                info=data
            )
            self.Qty_053 = convert_with_check(
                value=data['053 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_053_checker,
                info=data
            )
            self.Qty_062 = convert_with_check(
                value=data['062 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_062_checker,
                info=data
            )
            self.Qty_063 = convert_with_check(
                value=data['063 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_063_checker,
                info=data
            )
            self.Qty_065 = convert_with_check(
                value=data['065 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_065_checker,
                info=data
            )
            self.Qty_068 = convert_with_check(
                value=data['068 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_068_checker,
                info=data
            )
            self.Qty_069 = convert_with_check(
                value=data['069 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_069_checker,
                info=data
            )
            self.Qty_070 = convert_with_check(
                value=data['070 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_070_checker,
                info=data
            )
            self.Qty_071 = convert_with_check(
                value=data['071 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_071_checker,
                info=data
            )
            self.Qty_072 = convert_with_check(
                value=data['072 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_072_checker,
                info=data
            )
            self.Qty_077 = convert_with_check(
                value=data['077 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_077_checker,
                info=data
            )
            self.Qty_093 = convert_with_check(
                value=data['093 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_093_checker,
                info=data
            )
            self.Qty_094 = convert_with_check(
                value=data['094 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_094_checker,
                info=data
            )
            self.Qty_098 = convert_with_check(
                value=data['098 Qty'].split('.')[0],
                output_type=int,
                checker=self.Qty_098_checker,
                info=data
            )
            self.Discontinued = True if data['Discontinued'] == 'YES' \
                else False if data['Discontinued'] == 'NO' else None

        else:  # Разбор pricing
            mpn = convert_with_check(
                value=data['Meyer SKU'][3:],
                output_type=str,
                checker=self.mpn_checker,
                replace_if_false=False,
                warn_if_false=True,
                replace_if_conversion_error=False,
                warn_if_conversion_error=True,
                replace_if_checker_error=False,
                warn_if_checker_error=True,
                info=data
            )
            self.mpn = constants.delete_unprintable(mpn)

            brand = convert_with_check(
                value=data['Manufacturer Name'],
                output_type=str,
                checker=self.brand_checker,
                replace_if_false=False,
                warn_if_false=True,
                replace_if_conversion_error=False,
                warn_if_conversion_error=True,
                replace_if_checker_error=False,
                warn_if_checker_error=True,
                info=data
            )
            self.brand = constants.delete_unprintable(brand)

            prefix = convert_with_check(
                value=data['Meyer SKU'][:3],
                output_type=str,
                checker=self.prefix_checker,
                replace_if_false=False,
                warn_if_false=False,
                replace_if_conversion_error=False,
                warn_if_conversion_error=True,
                replace_if_checker_error=False,
                warn_if_checker_error=True,
                info=data
            )
            self.prefix = constants.delete_unprintable(prefix)

            self.number = self.prefix + self.mpn
            self.norm_mpn = constants.normalize_number(self.mpn)
            self.norm_brand = constants.normalize_brand(self.brand)

            if full_parse:
                self.Description = convert_with_check(
                    value=data['Description'],
                    output_type=str,
                    checker=self.Description_checker,
                    info=data
                )

                self.Jobber_Price = convert_with_check(
                    value=data['Jobber Price'],
                    output_type=float,
                    checker=self.Jobber_Price_checker,
                    info=data
                )
                self.Customer_Price = convert_with_check(
                    value=data['Customer Price'],
                    output_type=float,
                    checker=self.Customer_Price_checker,
                    info=data
                )
                self.UPC = convert_with_check(
                    value=data['UPC'],
                    output_type=str,
                    checker=self.UPC_checker,
                    info=data
                )
                self.MAP = convert_with_check(
                    value=data['MAP'],
                    output_type=float,
                    checker=self.MAP_checker,
                    info=data
                )
                self.Length = convert_with_check(
                    value=data['Length'],
                    output_type=float,
                    checker=self.Length_checker,
                    info=data
                )
                self.Width = convert_with_check(
                    value=data['Width'],
                    output_type=float,
                    checker=self.Width_checker,
                    info=data
                )
                self.Height = convert_with_check(
                    value=data['Height'],
                    output_type=float,
                    checker=self.Height_checker,
                    info=data
                )
                self.Weight = convert_with_check(
                    value=data['Weight'],
                    output_type=float,
                    checker=self.Weight_checker,
                    info=data
                )
                self.Category = convert_with_check(
                    value=data['Category'],
                    output_type=str,
                    checker=self.Category_checker,
                    info=data
                )
                self.Sub_Category = convert_with_check(
                    value=data['Sub-Category'],
                    output_type=str,
                    checker=self.Sub_Category_checker,
                    info=data
                )
                self.LTL_Eligible = True if data['LTL Eligible'] == 'True' \
                    else False if data['LTL Eligible'] == 'False' else None
                self.Discontinued = True if data['Discontinued'] == 'YES' \
                    else False if data['Discontinued'] == 'NO' else None

        super().__init__()


class PremierItem(BaseItem):
    Distributor_Cost_checker = Core_Price_checker = RangeChecker(0, 999999.9999)
    Package_Quantity_checker = Inventory_Count_checker = RangeChecker(0, 16777215)
    UPC_checker = MaxLenChecker(100)
    Part_Description_checker = MaxLenChecker(5000)
    Inventory_Type_checker = EnumChecker(('Discontinued', 'NonStocking', 'Stocking'))

    def __init__(self, data: Dict[str, str], *, full_parse=False) -> None:
        brand = convert_with_check(
            value=data['Brand'],
            output_type=str,
            checker=self.brand_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.brand = constants.delete_unprintable(brand)

        prefix = convert_with_check(
            value=data['Line Code'],
            output_type=str,
            checker=self.prefix_checker,
            replace_if_false=False,
            warn_if_false=False,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.prefix = constants.delete_unprintable(prefix)

        mpn = convert_with_check(
            value=data['SKU'].replace(prefix, '', 1),
            output_type=str,
            checker=self.mpn_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.mpn = constants.delete_unprintable(mpn)

        self.number = self.prefix + self.mpn
        self.norm_mpn = constants.normalize_number(self.mpn)
        self.norm_brand = constants.normalize_brand(self.brand)

        if full_parse:
            self.Distributor_Cost = convert_with_check(
                value=data['Distributor Cost'],
                output_type=float,
                checker=self.Distributor_Cost_checker,
                info=data
            )
            self.Package_Quantity = convert_with_check(
                value=data['Package Quantity'],
                output_type=int,
                checker=self.Package_Quantity_checker,
                info=data
            )
            self.Core_Price = convert_with_check(
                value=data['Core Price'],
                output_type=float,
                checker=self.Core_Price_checker,
                info=data
            )
            self.UPC = convert_with_check(
                value=data['UPC'],
                output_type=str,
                checker=self.UPC_checker,
                info=data
            )
            self.Part_Description = convert_with_check(
                value=data['Part Description'],
                output_type=str,
                checker=self.Part_Description_checker,
                info=data
            )
            self.Inventory_Count = convert_with_check(
                value=data['Inventory Count'],
                output_type=int,
                checker=self.Inventory_Count_checker,
                info=data
            )
            self.Inventory_Type = convert_with_check(
                value=data['Inventory Type'],
                output_type=str,
                checker=self.Inventory_Type_checker,
                info=data
            )

        super().__init__()


class TransItem(BaseItem):
    CA_checker = TX_checker = FL_checker = CO_checker = OH_checker = ID_checker = PA_checker = \
        TOTAL_checker = RangeChecker(0, 16777215)
    LIST_PRICE_checker = JOBBER_PRICE_checker = RangeChecker(0, 999999.9999)
    STATUS_checker = EnumChecker(('A', 'B', 'D', 'K', 'M', 'N', 'R'))

    def __init__(self, data: Dict[str, str], *, full_parse=False) -> None:
        prefix = convert_with_check(
            value=data['LINE'],
            output_type=str,
            checker=self.prefix_checker,
            replace_if_false=False,
            warn_if_false=False,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.prefix = constants.delete_unprintable(prefix)

        mpn = convert_with_check(
            value=data['PART_NUMBER_FULL'].replace(prefix, '', 1),
            output_type=str,
            checker=self.mpn_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.mpn = constants.delete_unprintable(mpn)

        brand = convert_with_check(
            value=trans_code.get(self.prefix, 'NOT DEFINED IN TRANS WITH PREFIX  ' + self.prefix),
            output_type=str,
            checker=self.brand_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.brand = constants.delete_unprintable(brand)

        self.number = self.prefix + self.mpn
        self.norm_mpn = constants.normalize_number(self.mpn)
        self.norm_brand = constants.normalize_brand(self.brand)

        if full_parse:
            self.CA = convert_with_check(
                data['CA'],
                output_type=int,
                checker=self.CA_checker,
                info=data
            )
            self.TX = convert_with_check(
                data['TX'],
                output_type=int,
                checker=self.TX_checker,
                info=data
            )
            self.FL = convert_with_check(
                data['FL'],
                output_type=int,
                checker=self.FL_checker,
                info=data
            )
            self.CO = convert_with_check(
                data['CO'],
                output_type=int,
                checker=self.CO_checker,
                info=data
            )
            self.OH = convert_with_check(
                data['OH'],
                output_type=int,
                checker=self.OH_checker,
                info=data
            )
            self.ID = convert_with_check(
                data['ID'],
                output_type=int,
                checker=self.ID_checker,
                info=data
            )
            self.PA = convert_with_check(
                data['PA'],
                output_type=int,
                checker=self.PA_checker,
                info=data
            )
            self.LIST_PRICE = convert_with_check(
                data['LIST_PRICE'],
                output_type=float,
                checker=self.LIST_PRICE_checker,
                info=data
            )
            self.JOBBER_PRICE = convert_with_check(
                data['JOBBER_PRICE'],
                output_type=float,
                checker=self.JOBBER_PRICE_checker,
                info=data
            )
            self.TOTAL = convert_with_check(
                data['TOTAL'],
                output_type=int,
                checker=self.TOTAL_checker,
                info=data
            )
            self.STATUS = convert_with_check(
                data['STATUS'],
                output_type=str,
                checker=self.STATUS_checker,
                info=data
            )

        super().__init__()


class Turn14Item(BaseItem):
    Description_checker = MaxLenChecker(5000)
    Cost_checker = Retail_checker = Jobber_checker = CoreCharge_checker = Map_checker = \
        Other_checker = RangeChecker(0, 999999.9999)
    OtherName_checker = EnumChecker(('Dealer', 'Net Dealer', 'WD'))
    EastStock_checker = WestStock_checker = CentralStock_checker = Stock_checker = \
        MfrStock_checker = RangeChecker(0, 16777215)
    MfrStockDate_checker = ...
    DropShip_checker = EnumChecker(('always', 'never', 'possible'))
    DSFee_checker = MaxLenChecker(100)
    Weight_checker = RangeChecker(0, 999999.99)

    def __init__(self, data: Dict[str, str], *, full_parse=False) -> None:
        mpn = convert_with_check(
            value=data['PartNumber'],
            output_type=str,
            checker=self.mpn_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.mpn = constants.delete_unprintable(mpn)

        brand = convert_with_check(
            value=data['PrimaryVendor'],
            output_type=str,
            checker=self.brand_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.brand = constants.delete_unprintable(brand)

        prefix = convert_with_check(
            value=''.join(data['InternalPartNumber'].rsplit(mpn, 1)),
            output_type=str,
            checker=self.prefix_checker,
            replace_if_false=False,
            warn_if_false=False,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.prefix = constants.delete_unprintable(prefix)

        self.number = self.prefix + self.mpn
        self.norm_mpn = constants.normalize_number(self.mpn)
        self.norm_brand = constants.normalize_brand(self.brand)

        if full_parse:
            self.Description = convert_with_check(
                value=data['Description'],
                output_type=str,
                checker=self.Description_checker,
                info=data
            )
            self.Cost = convert_with_check(
                value=data['Cost'].replace(',', ''),
                output_type=float,
                checker=self.Cost_checker,
                info=data
            )
            self.Retail = convert_with_check(
                value=data['Retail'].replace(',', ''),
                output_type=float,
                checker=self.Retail_checker,
                info=data
            )
            self.Jobber = convert_with_check(
                value=data['Jobber'].replace(',', ''),
                output_type=float,
                checker=self.Jobber_checker,
                info=data
            )
            self.CoreCharge = convert_with_check(
                value=data['CoreCharge'].replace(',', ''),
                output_type=float,
                checker=self.CoreCharge_checker,
                info=data
            )
            self.Map = convert_with_check(
                value=data['Map'].replace(',', ''),
                output_type=float,
                checker=self.Map_checker,
                info=data
            )
            self.Other = convert_with_check(
                value=data['Other'].replace(',', ''),
                output_type=float,
                checker=self.Other_checker,
                info=data
            )
            self.OtherName = convert_with_check(
                value=data['OtherName'],
                output_type=str,
                checker=self.OtherName_checker,
                info=data
            )
            self.EastStock = convert_with_check(
                value=data['EastStock'],
                output_type=int,
                checker=self.EastStock_checker,
                info=data
            )
            self.WestStock = convert_with_check(
                value=data['WestStock'],
                output_type=int,
                checker=self.WestStock_checker,
                info=data
            )
            self.CentralStock = convert_with_check(
                value=data['CentralStock'],
                output_type=int,
                checker=self.CentralStock_checker,
                info=data
            )
            self.Stock = convert_with_check(
                value=data['Stock'],
                output_type=int,
                checker=self.Stock_checker,
                info=data
            )
            self.MfrStock = convert_with_check(
                value=data['MfrStock'],
                output_type=int,
                checker=self.MfrStock_checker,
                info=data
            )
            try:
                _MfrStockDate = datetime.datetime.strptime(data['MfrStockDate'], '%Y-%m-%d').date()
            except ValueError:
                _MfrStockDate = None
            self.MfrStockDate = _MfrStockDate
            self.DropShip = convert_with_check(
                value=data['DropShip'],
                output_type=str,
                checker=self.DropShip_checker,
                info=data
            )
            self.DSFee = convert_with_check(
                value=data['DSFee'],
                output_type=str,
                checker=self.DSFee_checker,
                info=data
            )
            self.Weight = convert_with_check(
                value=data['Weight'].replace(',', ''),
                output_type=float,
                checker=self.Weight_checker,
                info=data
            )

        super().__init__()


class AutocareBrandItem(BaseItem):
    brand_id_checker = owner_id_checker = parent_id_checker = MaxLenChecker(4)
    brand_name_checker = owner_name_checker = parent_name_checker = MaxLenChecker(256)

    def __init__(self, data: dict) -> None:
        self.brand_id = convert_with_check(
            value=data['BrandID'],
            output_type=str,
            checker=self.brand_id_checker,
            replace_if_false=False,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.brand_name = convert_with_check(
            value=data['BrandName'],
            output_type=str,
            checker=self.brand_name_checker,
            replace_if_false=False,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.owner_id = convert_with_check(
            value=data.get('BrandOwnerID'),
            output_type=str,
            checker=self.owner_id_checker,
            info=data
        )
        self.owner_name = convert_with_check(
            value=data.get('BrandOwner'),
            output_type=str,
            checker=self.owner_name_checker,
            info=data
        )
        self.parent_id = convert_with_check(
            value=data.get('ParentID'),
            output_type=str,
            checker=self.parent_id_checker,
            info=data
        )
        self.parent_name = convert_with_check(
            value=data.get('ParentCompany'),
            output_type=str,
            checker=self.parent_name_checker,
            info=data
        )

        super().__init__()


class BrandAutocareBrandItem(AutocareBrandItem):
    name_checker = MaxLenChecker(256)

    def __init__(self, data: dict) -> None:
        name = convert_with_check(
            value=data.pop('name', None),
            output_type=str,
            checker=self.name_checker
        )
        if name:
            name = constants.delete_unprintable(name)
            self.norm_name = constants.normalize_brand(name)
        else:
            self.norm_name = None

        super().__init__(data)


class ExactBrandItem(BaseItem):
    exact_name_checker = norm_name_checker = MaxLenChecker(256)

    def __init__(self, data: dict) -> None:
        name = convert_with_check(
            value=data['name'],
            output_type=str,
            checker=self.norm_name_checker
        )
        if name:
            name = constants.delete_unprintable(name)
            self.norm_name = constants.normalize_brand(name)
        else:
            self.norm_name = None

        self.exact_name = convert_with_check(
            value=data['exact_name'],
            output_type=str,
            checker=self.exact_name_checker
        )
        super().__init__()


class MeyerAPIInformationItem(BaseItem):
    ItemNumber_checker = MaxLenChecker(266)
    CustomerPrice_checker = JobberPrice_checker = MinAdvertisedPrice_checker = SuggestedRetailPrice_checker = \
        RangeChecker(0, 999999.9999)
    Height_checker = Length_checker = Weight_checker = Width_checker = RangeChecker(0, 999999.99)
    QtyAvailable_checker = RangeChecker(0, 16777215)
    UPC_checker = MaxLenChecker(100)
    ItemDescription_checker = MaxLenChecker(5000)

    def __init__(self, data: dict) -> None:
        self.Additional_Handling_Charge = True if data['Additional Handling Charge'] == 'Yes' \
            else False if data['Additional Handling Charge'] == 'No' else None

        self.CustomerPrice = convert_with_check(
            value=data['CustomerPrice'],
            output_type=float,
            checker=self.CustomerPrice_checker,
            replace_if_false=True,
            replace_if_conversion_error=True,
            warn_if_conversion_error=True,
            replace_if_checker_error=True,
            warn_if_checker_error=True,
            info=data
        )

        self.Height = convert_with_check(
            value=data['Height'],
            output_type=float,
            checker=self.Height_checker,
            replace_if_false=True,
            replace_if_conversion_error=True,
            warn_if_conversion_error=True,
            replace_if_checker_error=True,
            warn_if_checker_error=True,
            info=data
        )

        self.ItemDescription = convert_with_check(
            value=data['ItemDescription'],
            output_type=str,
            checker=self.ItemDescription_checker,
            info=data
        )

        self.ItemNumber = convert_with_check(
            value=data['ItemNumber'],
            output_type=str,
            checker=self.ItemNumber_checker,
            replace_if_false=False,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )

        self.JobberPrice = convert_with_check(
            value=data['JobberPrice'],
            output_type=float,
            checker=self.JobberPrice_checker,
            replace_if_false=True,
            replace_if_conversion_error=True,
            warn_if_conversion_error=True,
            replace_if_checker_error=True,
            warn_if_checker_error=True,
            info=data
        )

        self.Kit = True if data['Kit'] == 'Yes' \
            else False if data['Kit'] == 'No' else None

        self.Kit_Only = True if data['Kit Only'] == 'Yes' \
            else False if data['Kit Only'] == 'No' else None

        self.LTL_Required = True if data['LTL Required'] == 'Yes' \
            else False if data['LTL Required'] == 'No' else None

        self.Length = convert_with_check(
            value=data['Length'],
            output_type=float,
            checker=self.Length_checker,
            replace_if_false=True,
            replace_if_conversion_error=True,
            warn_if_conversion_error=True,
            replace_if_checker_error=True,
            warn_if_checker_error=True,
            info=data
        )

        self.MinAdvertisedPrice = convert_with_check(
            value=data['MinAdvertisedPrice'],
            output_type=float,
            checker=self.MinAdvertisedPrice_checker,
            replace_if_false=True,
            replace_if_conversion_error=True,
            warn_if_conversion_error=True,
            replace_if_checker_error=True,
            warn_if_checker_error=True,
            info=data
        )

        self.Oversize = True if data['Oversize'] == 'Yes' \
            else False if data['Oversize'] == 'No' else None

        self.Discontinued = True if data['PartStatus'] == 'Discontinued' \
            else False if data['PartStatus'] == 'Active' else None  # вместо PartStatus

        self.QtyAvailable = convert_with_check(
            value=data['QtyAvailable'],
            output_type=int,
            checker=self.QtyAvailable_checker,
            info=data
        )

        self.SuggestedRetailPrice = convert_with_check(
            value=data['SuggestedRetailPrice'],
            output_type=float,
            checker=self.SuggestedRetailPrice_checker,
            replace_if_false=True,
            replace_if_conversion_error=True,
            warn_if_conversion_error=True,
            replace_if_checker_error=True,
            warn_if_checker_error=True,
            info=data
        )

        self.UPC = convert_with_check(
            value=data['UPC'],
            output_type=str,
            checker=self.UPC_checker,
            info=data
        )

        self.Weight = convert_with_check(
            value=data['Weight'],
            output_type=float,
            checker=self.Weight_checker,
            replace_if_false=True,
            replace_if_conversion_error=True,
            warn_if_conversion_error=True,
            replace_if_checker_error=True,
            warn_if_checker_error=True,
            info=data
        )

        self.Width = convert_with_check(
            value=data['Width'],
            output_type=float,
            checker=self.Width_checker,
            replace_if_false=True,
            replace_if_conversion_error=True,
            warn_if_conversion_error=True,
            replace_if_checker_error=True,
            warn_if_checker_error=True,
            info=data
        )

        super().__init__()


class PremierAPIPricingItem(BaseItem):
    premier_number_checker = MaxLenChecker(266)
    cost_usd_checker = cost_cad_checker = jobber_usd_checker = jobber_cad_checker = \
        map_usd_checker = map_cad_checker = retail_usd_checker = retail_cad_checker = RangeChecker(0, 999999.99)

    def __init__(self, data: dict) -> None:
        self.premier_number = convert_with_check(
            value=data['itemNumber'],
            output_type=str,
            checker=self.premier_number_checker,
            replace_if_false=False,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.cost_usd = self.jobber_usd = self.map_usd = self.retail_usd = \
            self.cost_cad = self.jobber_cad = self.map_cad = self.retail_cad = None
        pricing = data.get('pricing', [])
        pricing_usd = pricing_cad = None
        for p in pricing:
            if p.get('currency') == 'USD':
                pricing_usd = p
            elif p.get('currency') == 'CAD':
                pricing_cad = p
        if pricing_usd:
            self.cost_usd = convert_with_check(
                value=pricing_usd.get('cost'),
                output_type=float,
                checker=self.cost_usd_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
            self.jobber_usd = convert_with_check(
                value=pricing_usd.get('jobber'),
                output_type=float,
                checker=self.jobber_usd_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
            self.map_usd = convert_with_check(
                value=pricing_usd.get('map'),
                output_type=float,
                checker=self.map_usd_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
            self.retail_usd = convert_with_check(
                value=pricing_usd.get('retail'),
                output_type=float,
                checker=self.retail_usd_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
        if pricing_cad:
            self.cost_cad = convert_with_check(
                value=pricing_cad.get('cost'),
                output_type=float,
                checker=self.cost_cad_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
            self.jobber_cad = convert_with_check(
                value=pricing_cad.get('jobber'),
                output_type=float,
                checker=self.jobber_cad_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
            self.map_cad = convert_with_check(
                value=pricing_cad.get('map'),
                output_type=float,
                checker=self.map_cad_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
            self.retail_cad = convert_with_check(
                value=pricing_cad.get('retail'),
                output_type=float,
                checker=self.retail_cad_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )

        super().__init__()


# noinspection PyPep8Naming
class PremierAPIInventoryItem(BaseItem):
    premier_number_checker = MaxLenChecker(266)
    Qty_UT_1_US_checker = Qty_KY_1_US_checker = Qty_TX_1_US_checker = Qty_CA_1_US_checker = Qty_AB_1_CA_checker = \
        Qty_WA_1_US_checker = Qty_CO_1_US_checker = Qty_PO_1_CA_checker = RangeChecker(0, 16777215)

    def __init__(self, data: dict) -> None:
        self.premier_number = convert_with_check(
            value=data['itemNumber'],
            output_type=str,
            checker=self.premier_number_checker,
            replace_if_false=False,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.Qty_UT_1_US = self.Qty_KY_1_US = self.Qty_TX_1_US = self.Qty_CA_1_US = \
            self.Qty_AB_1_CA = self.Qty_WA_1_US = self.Qty_CO_1_US = self.Qty_PO_1_CA = None
        inventory = data.get('inventory', [])
        inventory_Qty_UT_1_US = inventory_Qty_KY_1_US = inventory_Qty_TX_1_US = inventory_Qty_CA_1_US = \
            inventory_Qty_AB_1_CA = inventory_Qty_WA_1_US = inventory_Qty_CO_1_US = inventory_Qty_PO_1_CA = None
        for i in inventory:
            if i.get('warehouseCode') == 'UT-1-US':
                inventory_Qty_UT_1_US = i
            elif i.get('warehouseCode') == 'KY-1-US':
                inventory_Qty_KY_1_US = i
            elif i.get('warehouseCode') == 'TX-1-US':
                inventory_Qty_TX_1_US = i
            elif i.get('warehouseCode') == 'CA-1-US':
                inventory_Qty_CA_1_US = i
            elif i.get('warehouseCode') == 'AB-1-CA':
                inventory_Qty_AB_1_CA = i
            elif i.get('warehouseCode') == 'WA-1-US':
                inventory_Qty_WA_1_US = i
            elif i.get('warehouseCode') == 'CO-1-US':
                inventory_Qty_CO_1_US = i
            elif i.get('warehouseCode') == 'PO-1-CA':
                inventory_Qty_PO_1_CA = i
        if inventory_Qty_UT_1_US:
            self.Qty_UT_1_US = convert_with_check(
                value=inventory_Qty_UT_1_US.get('quantityAvailable'),
                output_type=int,
                checker=self.Qty_UT_1_US_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
        if inventory_Qty_KY_1_US:
            self.Qty_KY_1_US = convert_with_check(
                value=inventory_Qty_KY_1_US.get('quantityAvailable'),
                output_type=int,
                checker=self.Qty_KY_1_US_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
        if inventory_Qty_TX_1_US:
            self.Qty_TX_1_US = convert_with_check(
                value=inventory_Qty_TX_1_US.get('quantityAvailable'),
                output_type=int,
                checker=self.Qty_TX_1_US_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
        if inventory_Qty_CA_1_US:
            self.Qty_CA_1_US = convert_with_check(
                value=inventory_Qty_CA_1_US.get('quantityAvailable'),
                output_type=int,
                checker=self.Qty_CA_1_US_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
        if inventory_Qty_AB_1_CA:
            self.Qty_AB_1_CA = convert_with_check(
                value=inventory_Qty_AB_1_CA.get('quantityAvailable'),
                output_type=int,
                checker=self.Qty_AB_1_CA_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
        if inventory_Qty_WA_1_US:
            self.Qty_WA_1_US = convert_with_check(
                value=inventory_Qty_WA_1_US.get('quantityAvailable'),
                output_type=int,
                checker=self.Qty_WA_1_US_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
        if inventory_Qty_CO_1_US:
            self.Qty_CO_1_US = convert_with_check(
                value=inventory_Qty_CO_1_US.get('quantityAvailable'),
                output_type=int,
                checker=self.Qty_CO_1_US_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )
        if inventory_Qty_PO_1_CA:
            self.Qty_PO_1_CA = convert_with_check(
                value=inventory_Qty_PO_1_CA.get('quantityAvailable'),
                output_type=int,
                checker=self.Qty_PO_1_CA_checker,
                replace_if_false=False,
                warn_if_conversion_error=True,
                warn_if_checker_error=True,
                info=data
            )

        super().__init__()


class Turn14APIAllItemsItem(BaseItem):
    item_id_in_api_checker = MaxLenChecker(20)
    product_name_checker = category_checker = subcategory_checker = \
        dimensions_checker = thumbnail_checker = MaxLenChecker(256)
    number_checker = MaxLenChecker(266)
    barcode_checker = MaxLenChecker(50)

    def __init__(self, data: dict) -> None:
        self.item_id_in_api = convert_with_check(
            value=data.get('id'),
            output_type=str,
            checker=self.item_id_in_api_checker,
            warn_if_false=True,
            warn_if_conversion_error=True,
            warn_if_checker_error=True,
            info=data
        )
        attributes = data.get('attributes', {})
        self.product_name = convert_with_check(
            value=attributes.get('product_name'),
            output_type=str,
            checker=self.product_name_checker
        )
        number = convert_with_check(
            value=attributes['part_number'],
            output_type=str,
            checker=self.number_checker,
            warn_if_false=True,
            warn_if_conversion_error=True,
            warn_if_checker_error=True
        )
        self.number = constants.delete_unprintable(number) if number else None
        self.category = convert_with_check(
            value=attributes.get('category'),
            output_type=str,
            checker=self.category_checker
        )
        self.subcategory = convert_with_check(
            value=attributes.get('subcategory'),
            output_type=str,
            checker=self.subcategory_checker
        )
        self.dimensions = convert_with_check(
            ' | '.join(
                '{}: {} x {} x {} - {}'.format(
                    d.get('box_number', ''),
                    d.get('length', ''),
                    d.get('width', ''),
                    d.get('height', ''),
                    d.get('weight', '')
                ) for d in attributes.get('dimensions', [])
            ),
            output_type=str,
            checker=self.dimensions_checker
        )
        self.thumbnail = convert_with_check(
            value=attributes.get('thumbnail'),
            output_type=str,
            checker=self.thumbnail_checker
        )
        self.barcode = convert_with_check(
            value=attributes.get('barcode'),
            output_type=str,
            checker=self.barcode_checker
        )

        super().__init__()


class Turn14APIAllItemDataItem(BaseItem):
    item_id_in_api_checker = MaxLenChecker(20)
    media_content_checker = url_checker = MaxLenChecker(256)
    height_checker = width_checker = RangeChecker(0, 65535)
    vehicle_id_checker = RangeChecker(0, 2147483647)

    def __init__(self, data: dict) -> None:
        self.item_id_in_api = convert_with_check(
            value=data.get('id'),
            output_type=str,
            checker=self.item_id_in_api_checker,
            warn_if_false=True,
            warn_if_conversion_error=True,
            warn_if_checker_error=True,
            info=data
        )

        self.files = []
        files = data.get('files', [])
        f = {}
        for file in files:
            media_content = convert_with_check(
                value=file.get('media_content'),
                output_type=str,
                checker=self.media_content_checker
            )
            links = file.get('links', [])
            for link in links:
                url = convert_with_check(
                    value=link.get('url'),
                    output_type=str,
                    checker=self.url_checker,
                    warn_if_false=True,
                    replace_if_checker_error=False,
                    warn_if_checker_error=True
                )
                height = convert_with_check(
                    value=link.get('height'),
                    output_type=float,
                    checker=self.height_checker
                )
                width = convert_with_check(
                    value=link.get('width'),
                    output_type=float,
                    checker=self.width_checker
                )
                if url:
                    f[url] = {
                        'url': url,
                        'media_content': media_content,
                        'height': int(height) if height else None,
                        'width': int(width) if width else None
                    }
        self.files = list(f.values())

        self.vehicle_fitments_ids = []
        vehicle_fitments = data.get('vehicle_fitments', [])
        v = set()
        for vehicle_fitment in vehicle_fitments:
            vehicle_id = convert_with_check(
                value=vehicle_fitment.get('vehicle_id'),
                output_type=int,
                checker=self.vehicle_id_checker,
            )
            if vehicle_id:
                v.add(vehicle_id)
        self.vehicle_fitments_ids = list(v)

        super().__init__()


class TransFileItem(BaseItem):
    Description_checker = MaxLenChecker(5000)
    Your_Price_checker = Jobber_checker = MAP_CONFIRM_W_JOBBER_checker = \
        Core_Price_checker = Federal_Excise_Tax_checker = RangeChecker(0, 999999.9999)
    Status_checker = EnumChecker(('A', 'B', 'D', 'K', 'M', 'N', 'R'))

    def __init__(self, data: Dict[str, str]) -> None:
        """

        :rtype: object
        """
        prefix = convert_with_check(
            value=data['Line Code'],
            output_type=str,
            checker=self.prefix_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.prefix = constants.delete_unprintable(prefix)

        mpn = convert_with_check(
            value=data['TAW PN'].replace(prefix, '', 1),
            output_type=str,
            checker=self.mpn_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.mpn = constants.delete_unprintable(mpn)

        brand = convert_with_check(
            value=trans_code.get(self.prefix, 'NOT DEFINED IN TRANS WITH PREFIX  ' + self.prefix),
            output_type=str,
            checker=self.brand_checker,
            replace_if_false=False,
            warn_if_false=True,
            replace_if_conversion_error=False,
            warn_if_conversion_error=True,
            replace_if_checker_error=False,
            warn_if_checker_error=True,
            info=data
        )
        self.brand = constants.delete_unprintable(brand)

        self.number = self.prefix + self.mpn
        self.norm_mpn = constants.normalize_number(self.mpn)
        self.norm_brand = constants.normalize_brand(self.brand)

        self.Description = convert_with_check(
            value=data['Description'],
            output_type=str,
            checker=self.Description_checker,
            info=data
        )

        self.Your_Price = convert_with_check(
            value=data['Your Price'].replace(',', '.').replace(' ', ''),
            output_type=float,
            checker=self.Your_Price_checker,
            info=data
        )

        self.Jobber = convert_with_check(
            value=data['Jobber'].replace(',', '.').replace(' ', ''),
            output_type=float,
            checker=self.Jobber_checker,
            info=data
        )

        self.MAP_CONFIRM_W_JOBBER = convert_with_check(
            value=data['MAP-CONFIRM W JOBBER'].replace(',', '.').replace(' ', ''),
            output_type=float,
            checker=self.MAP_CONFIRM_W_JOBBER_checker,
            info=data
        )

        self.Core_Price = convert_with_check(
            value=data['Core Price'].replace(',', '.').replace(' ', ''),
            output_type=float,
            checker=self.Core_Price_checker,
            info=data
        )

        self.Federal_Excise_Tax = convert_with_check(
            value=data['Federal Excise Tax'].replace(',', '.').replace(' ', ''),
            output_type=float,
            checker=self.Federal_Excise_Tax_checker,
            info=data
        )

        self.Oversize = True if data['Oversize'] == 'Y' else False if data['Oversize'] == 'N' else None

        self.Status = convert_with_check(
            value=data['Status Code'],
            output_type=str,
            checker=self.Status_checker,
            info=data,
            warn_if_checker_error=True,
            warn_if_conversion_error=True,
            warn_if_false=True
        )

        super().__init__()
