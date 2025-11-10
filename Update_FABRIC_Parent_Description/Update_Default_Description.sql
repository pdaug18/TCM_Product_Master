/*Request: Product Data Coordinator 
we need to have a description added for these parent skus.
The logic should be if the parent sku contains "FABRIC" then the Parent Description = Product Description.

I have attached an excel file with the data- Update_FABRIC_Parent_Description.xlsx
*/

describe table SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE;
/*
+---------------------------+------------------+
| Column Name               | Data Type        |
+---------------------------+------------------+
| ALT_KEY                   | VARCHAR(15)      |
| ATTR (PAR) BERRY          | VARCHAR(75)      |
| ATTR (PAR) CARE           | VARCHAR(75)      |
| ATTR (PAR) HEAT TRANSFER  | VARCHAR(75)      |
| ATTR (PAR) OTHER          | VARCHAR(75)      |
| ATTR (PAR) PAD PRINT      | VARCHAR(75)      |
| ATTR (PAR) PRODUCT CAT    | VARCHAR(75)      |
| ATTR (PAR) PRODUCT TYPE   | VARCHAR(75)      |
| ATTR (PAR) TRACKING       | VARCHAR(75)      |
| ATTR (PAR) Z_BRAND        | VARCHAR(75)      |
| ATTR (PAR) Z_CATEGORY     | VARCHAR(75)      |
| ATTR (PAR) Z_GENDER       | VARCHAR(75)      |
| ATTR (PAR) Z_VERTICAL     | VARCHAR(75)      |
| ATTR (SKU) CERT_NUM       | VARCHAR(75)      |
| ATTR (SKU) COLOR          | VARCHAR(75)      |
| ATTR (SKU) LENGTH         | VARCHAR(75)      |
| ATTR (SKU) PFAS           | VARCHAR(75)      |
| ATTR (SKU) SIZE           | VARCHAR(75)      |
| ATTR (SKU) TARIFF_CODE    | VARCHAR(75)      |
| ATTR (SKU) UPC_CODE       | VARCHAR(75)      |
| Booking Type Table        | VARCHAR(16777216)|
| CATEGORY (Calc)           | VARCHAR(29)      |
| COST CAT DESCR            | VARCHAR(35)      |
| COST CATEGORY ID          | VARCHAR(2)       |
| ID_LOC                    | VARCHAR(5)       |
| PARENT DESCRIPTION        | VARCHAR(16777216)|
| PRDT CAT DESCR            | VARCHAR(24)      |
| PRODUCT CATEGORY/VERTICAL | VARCHAR(2)       |
| Product Description       | VARCHAR(61)      |
| Product ID/SKU            | VARCHAR(30)      |
| Product Name/Parent ID    | VARCHAR(75)      |
| PROP 65                   | VARCHAR(1)       |
| VERTICAL (Calc)           | VARCHAR(14)      |
+---------------------------+------------------+
*/

select * from SILVER_DATA.TCM_SILVER.MASTER_PRODUCT_TABLE limit 10;
/*
/*
┌─────────────────────┬───────────────────────────────────────────────────┬──────────────────┬─────────────────────┬───────────────────────────┬─────────────────────┬─────────────────┬────────────────────┬────────────────────┬─────────────────────────────────┐
│ Product ID/SKU      │ Product Description                               │ COST CATEGORY ID │ COST CAT DESCR      │ PRODUCT CATEGORY/VERTICAL │ PRDT CAT DESCR      │ VERTICAL (Calc) │ CATEGORY (Calc)    │ Product Name/      │ PARENT DESCRIPTION              │
│                     │                                                   │                  │                     │                           │                     │                 │                    │ Parent ID          │                                 │
├─────────────────────┼───────────────────────────────────────────────────┼──────────────────┼─────────────────────┼───────────────────────────┼─────────────────────┼─────────────────┼────────────────────┼────────────────────┼─────────────────────────────────┤
│ FDX17122420         │ Dual Certified Jacket - Tan - Large Regular       │ ZZ               │ ZZ - NEEDS REVIEW   │ 25                        │ 25 - MILITARY       │ null            │ null               │ null               │ MISSING DESCRIPTION - UPDATE TCM│
│ C22NU2X50198ST      │ COAT, 13 OZ ALUMINIZED CARBON KEVLAR, OVERLAP...  │ 05               │ 05 - FINISHED GOODS │ 15                        │ 15 - THERMAL        │ THERMAL         │ CLOTHING           │ C22NU198ST         │ MISSING DESCRIPTION - UPDATE TCM│
│ KIT2NC08AGB         │ LEVEL II KIT - NO COVERALL INCLUDES THE FOLLOW... │ 05               │ 05 - FINISHED GOODS │ 10                        │ 10 - ARC PPE        │ ARC FLASH PPE   │ CLOTHING & KITS    │ KIT2NC             │ CAT 2 Arc Flash Accessory Kit...│
│ 25309-06-30X34      │ UNION LINE CARP JEAN 30X34                        │ 05               │ 05 - FINISHED GOODS │ 35                        │ 35 - USA/UNION      │ AD SPECIALTY    │ #NOT CATEGORIZED   │ 25309-06           │ MISSING DESCRIPTION - UPDATE TCM│
│ DF2-CM-450C-LS-...  │ DRIFIRE 4.4 FR WORK SHIRT 8 CAL 4.4OZ NAVY...     │ 05               │ 05 - FINISHED GOODS │ 05                        │ 05 - FRC            │ INDUSTRIAL PPE  │ #NOT CATEGORIZED   │ DF2-CM-450C-LS-... │ MISSING DESCRIPTION - UPDATE TCM│
│ C04UP03LG49         │ ENESPRO ARCGUARD 12 CAL COAT LG49                 │ 05               │ 05 - FINISHED GOODS │ 10                        │ 10 - ARC PPE        │ ARC FLASH PPE   │ CLOTHING & KITS    │ C04UP03            │ ArcGuard® 12 cal Arc Flash Coat │
│ B01HG90D6ID         │ BELLOW BOOT,HYPALON COATED GREEN NYLON,16OZ...    │ ZZ               │ ZZ - NEEDS REVIEW   │ B                         │ INVALID PRODUCT CAT │ INDUSTRIAL PPE  │ #NOT CATEGORIZED   │                    │ MISSING DESCRIPTION - UPDATE TCM│
│ G04H5VB23           │ GLOVE, 9 OZ NAPPED KEVLAR PALM 11OZ ALUM...       │ 05               │ 05 - FINISHED GOODS │ 15                        │ 15 - THERMAL        │ THERMAL         │ HAND PROTECTION    │ G04H5VB            │ CARBON ARMOUR™ Alum Glove...    │
│ VNCSETUP            │ Screen Print Set Up Charge                        │ 65               │ 65 - NON-VALUED RM  │ 60                        │ 60 - OTHER (RM)     │ null            │ null               │ null               │ MISSING DESCRIPTION - UPDATE TCM│
│ S20702              │ CLR 4x20 BLK9349L/ST#571=J4605                    │ 65               │ 65 - NON-VALUED RM  │ 60                        │ 60 - OTHER (RM)     │ INDUSTRIAL PPE  │ #NOT CATEGORIZED   │                    │ MISSING DESCRIPTION - UPDATE TCM│
└─────────────────────┴───────────────────────────────────────────────────┴──────────────────┴─────────────────────┴───────────────────────────┴─────────────────────┴─────────────────┴────────────────────┴────────────────────┴─────────────────────────────────┘
*/
*/