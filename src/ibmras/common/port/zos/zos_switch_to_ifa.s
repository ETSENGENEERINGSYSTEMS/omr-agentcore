*                                                                       00050000
* %DCL IEAVIFAI_INCLUDED CHAR EXT;                                      00100000
* %DEACTIVATE IEAVIFAI_INCLUDED;                                        00150000
* %IF IEAVIFAI_INCLUDED = '' %THEN                                      00200000
* %DO;                                                                  00250000
*/* Macro made bi-lingual on 03272. CBGEN compile date 03231         */ 00300000
*% /*                                                                   00350000
         MACRO                                                          00400000
         IEAVIFAI &DSECT=YES,&LIST=YES,&TITLE=YES                       00450000
                      GBLC  &IEAVIFAI_INCLUDED                          00500000
                      GBLC  &ZCBPRINT                                   00550000
&IEAVIFAI_INCLUDED    SETC  'YES'                                       00600000
                      AIF   ('&LIST' EQ 'NONE').P0                      00650000
                      AIF   ('&TITLE' EQ 'NO').P5                       00700000
 TITLE                'IEAVIFAI  - IEAVIFAx Interface information      *00750000
                                       '                                00800000
.P5                   ANOP                                              00850000
*/* START OF SPECIFICATIONS ******************************************* 00950000
*                                                                       01000000
*  **PROPRIETARY_STATEMENT********************************************  01050000
***01* PROPRIETARY STATEMENT=                                        *  01100000
*                                                                    *  01150000
*                                                                    *  01200000
*   LICENSED MATERIALS - PROPERTY OF IBM                             *  01250000
*   5694-A01 (C) COPYRIGHT IBM CORP. 2004                            *  01300000
*                                                                    *  01350000
*   STATUS= HBB7709                                                  *  01400000
*                                                                    *  01450000
*  **END_OF_PROPRIETARY_STATEMENT*************************************  01500000
*                                                                       01550000
*01* DESCRIPTIVE NAME:  IEAVIFAx Interface information                  01600000
*02*  ACRONYM:  IFAI                                                    01650000
*                                                                       01700000
*01* MACRO NAME:  IEAVIFAI                                              01750000
*                                                                       01800000
*01* EXTERNAL CLASSIFICATION: NONE                                      01850000
*01* END OF EXTERNAL CLASSIFICATION:                                    01900000
*                                                                       01950000
*01* DSECT NAME:                                                        02000000
*                                                                       02050000
*01* COMPONENT:  Supervisor Control (SC1C5)                             02100000
*                                                                       02150000
*01* EYE-CATCHER:  NONE                                                 02200000
*                                                                       02250000
*01* STORAGE ATTRIBUTES:                                                02300000
*02*  SUBPOOL:  Caller-supplied                                         02350000
*02*  KEY:  Caller-supplied                                             02400000
*02*  RESIDENCY:  Caller-supplied                                       02450000
*                                                                       02500000
*01* SIZE:  Variable                                                    02550000
*                                                                       02600000
*01* CREATED BY:                                                        02650000
*     Caller. Equates for users of IEAVIFAT/IEAVIFAF                    02700000
*      services                                                         02750000
*                                                                       02800000
*01* POINTED TO BY:                                                     02850000
*     N/A                                                               02900000
*                                                                       02950000
*01* SERIALIZATION:                                                     03000000
*     None required                                                     03050000
*                                                                       03100000
*01* FUNCTION:                                                          03150000
*02* Interface definition for IEAVIFA to switch to/from an IFA engine.  03200000
*    IEAVIFAT switches "to".                                            03250000
*    IEAVIFAF switches "from".                                          03300000
*                                                                       03350000
*01* METHOD OF ACCESS:                                                  03400000
*02*  ASM:                                                              03450000
*      IEAVIFAI                                                         03500000
*           DSECT=YES|NO  -- Request DSECT definition                   03550000
*     Default: DSECT=YES                                                03600000
*     Notes: name=YES  => expand                                        03650000
*            name=NO   => do not expand                                 03700000
*                                                                       03750000
*02*  PL/AS:                                                            03800000
*      %INCLUDE SYSLIB(IEAVIFAI)                                        03850000
*                                                                       03900000
*01* DELETED BY:  Caller                                                03950000
*                                                                       04000000
*01* FREQUENCY:  Created for use of IEAVIFAx services                   04050000
*                                                                       04100000
*01* DEPENDENCIES:  None                                                04150000
*                                                                       04200000
*01* DISTRIBUTION LIBRARY:  AINTLIB                                     04250000
*                                                                       04300000
*01* CHANGE ACTIVITY:                                                   04350000
*    $H0=IFA      HBB7709 031205 PD00XB: IFA support                    04400000
*                                                                       04450000
* END OF SPECIFICATIONS *********************************************/  04500000
*% /*                                                                   04550000
.P0                   ANOP                                              04600000
                      AIF   ('&ZCBPRINT' EQ 'NO').P1                    04650000
                      AIF   ('&LIST' EQ 'YES').P2                       04700000
.P1                   ANOP                                              04750000
                      PUSH  PRINT                                       04800000
                      PRINT OFF                                         04850000
.P2                   ANOP                                              04900000
IEAVIFA_TO_IFA        EQU   1                                           04950000
IEAVIFA_FROM_IFA      EQU   0                                           05000000
IEAVIFA_RCMASK        EQU   X'000000FF'                                 05050000
IEAVIFA_RSNMASK       EQU   X'FFFFFF00'                                 05100000
IEAVIFA_NOTAVAILABLE  EQU   X'0000010C'                                 05150000
IEAVIFA_NOTINJPQ      EQU   X'00000208'                                 05200000
IEAVIFA_BADCSVQUERY   EQU   X'00000308'                                 05250000
IEAVIFA_NOTFROMHFS    EQU   X'00000408'                                 05300000
IEAVIFA_WRONGPATHNAME EQU   X'00000508'                                 05350000
IEAVIFA_BADR7         EQU   X'00000608'                                 05400000
IEAVIFA_NOTAUTHLIB    EQU   X'00000708'                                 05450000
IEAVIFA_NOIFASONLINE  EQU   X'00000804'                                 05500000
IEAVIFA_NOTTASKMODE   EQU   X'00000908'                                 05550000
IEAVIFA_NOIFAS        EQU   X'00000A04'                                 05600000
IEAVIFA_SUCCESS       EQU   X'00000000'                                 05650000
                      AIF   ('&ZCBPRINT' EQ 'NO').P3                    05700000
                      AIF   ('&LIST' EQ 'YES').P4                       05750000
.P3                   ANOP                                              05800000
                      POP   PRINT                                       05850000
.P4                   ANOP                                              05900000
.P_EXIT               ANOP                                              05950000
                      MEND                                              06000000
**/ IEAVIFAI_2:;                                                        06050000
* %IEAVIFAI_INCLUDED = 'YES';                                           06100000
* %DCL ZCBPRINT CHAR EXT;                                               06150000
* %DEACTIVATE ZCBPRINT;                                                 06200000
* %DCL IEAVIFAI_LIST CHAR EXT;                                          06250000
* %DEACTIVATE IEAVIFAI_LIST;                                            06300000
* %IF IEAVIFAI_LIST ,= 'NO' &                                           06350000
*    ZCBPRINT ,= 'NO' %THEN                                             06400000
*   %GOTO IEAVIFAI_3;                                                   06450000
*   @LIST PUSH NOECHO;                                                  06500000
*   @LIST NOASSEMBLE NOECHO;                                            06550000
*   @LIST OFF C NOECHO;                                                 06600000
* %IEAVIFAI_3:;                                                         06650000
*/* Start of PL/X Source                                             */ 06700000
*DCL IEAVIFA_To_IFA Constant(1);                                        06750000
*DCL IEAVIFA_From_IFA Constant(0);                                      06800000
*%DCL EHAREGS CHAR EXT;                                                 06850000
*%IF INDEX(EHAREGS,'01P') = 0 %THEN                                     06900000
*  %EHAREGS = EHAREGS || '01P';                                         06950000
*%IF INDEX(EHAREGS,'02P') = 0 %THEN                                     07000000
*  %EHAREGS = EHAREGS || '02P';                                         07050000
*%IF INDEX(EHAREGS,'03P') = 0 %THEN                                     07100000
*  %EHAREGS = EHAREGS || '03P';                                         07150000
*%IF IEAVIFAT_Options_String = '' %THEN                                 07200000
*   %IEAVIFAT_Options_String =                                          07250000
*               '(ReturnCode:=Fixed byvalue output inreg(3))' ||        07300000
*               ' External Nonlocal' ||                                 07350000
*               ' options(entreg(6) retreg(7)' ||                       07400000
*               ' sets(GPR01P,GPR02P,GPR03P)' ||                        07450000
*               ' nosave(GPR01P,GPR02P,GPR03P))';                       07500000
*%IF IEAVIFAF_Options_String = '' %THEN                                 07550000
*   %IEAVIFAF_Options_String =                                          07600000
*               '(ReturnCode:=Fixed byvalue output inreg(3))' ||        07650000
*               ' External Nonlocal' ||                                 07700000
*               ' options(entreg(6) retreg(7)' ||                       07750000
*               ' sets(GPR01P,GPR02P,GPR03P)' ||                        07800000
*               ' nosave(GPR01P,GPR02P,GPR03P))';                       07850000
*DCL IEAVIFAT Entry IEAVIFAT_OPTIONS_STRING;                            07900000
*DCL IEAVIFAF Entry IEAVIFAF_OPTIONS_STRING;                            07950000
*DCL IEAVIFA_RCMask        BIT(32) Constant('000000FF'x);               08000000
*DCL IEAVIFA_RSNMask       BIT(32) Constant('FFFFFF00'x);               08050000
*DCL IEAVIFA_NotAvailable  BIT(32) Constant('0000010C'x);               08100000
*DCL IEAVIFA_NotInJPQ      BIT(32) Constant('00000208'x);               08150000
*DCL IEAVIFA_BadCSVQUERY   BIT(32) Constant('00000308'x);               08200000
*DCL IEAVIFA_NotFromHFS    BIT(32) Constant('00000408'x);               08250000
*DCL IEAVIFA_WrongPathname BIT(32) Constant('00000508'x);               08300000
*DCL IEAVIFA_BadR7         BIT(32) Constant('00000608'x);               08350000
*DCL IEAVIFA_NotAuthLib    BIT(32) Constant('00000708'x);               08400000
*DCL IEAVIFA_NoIFAsOnline  BIT(32) Constant('00000804'x);               08450000
*DCL IEAVIFA_NotTaskMode   BIT(32) Constant('00000908'x);               08500000
*DCL IEAVIFA_NoIFAs        BIT(32) Constant('00000A04'x);               08550000
*DCL IEAVIFA_Success       BIT(32) Constant('00000000'x);               08600000
*@LOGIC;                                                                08650000
*#PRAGMA TOOL=CBGEN.                                                    08700000
*#USEORG.                                                               08750000
*#ALIGNOP.                                                              08800000
*#EPRAGMA.                                                              08850000
*@ENDLOGIC;                                                             08900000
*/* End of PL/X Source                                               */ 08950000
* %IF IEAVIFAI_LIST ,= 'NO' &                                           09000000
*    ZCBPRINT ,= 'NO' %THEN                                             09050000
*   %GOTO IEAVIFAI_4;                                                   09100000
*   @LIST POP NOECHO;                                                   09150000
* %IEAVIFAI_4:;                                                         09200000
* %END;                                                                 09250000
*/* START OF SPECIFICATIONS ******************************************* 00050000
*                                                                       00100000
*  **PROPRIETARY_STATEMENT********************************************  00150000
***01* PROPRIETARY STATEMENT=                                        *  00200000
*                                                                    *  00250000
*                                                                    *  00300000
*   LICENSED MATERIALS - PROPERTY OF IBM                             *  00350000
*   5694-A01 (C) COPYRIGHT IBM CORP. 2004                            *  00400000
*                                                                    *  00450000
*   STATUS= HBB7709                                                  *  00500000
*                                                                    *  00550000
*  **END_OF_PROPRIETARY_STATEMENT*************************************  00600000
*                                                                       00650000
*01* DESCRIPTIVE NAME:  Produce stub for switch service                 00700000
*02*  ACRONYM:  NONE                                                    00750000
*                                                                       00800000
*01* MACRO NAME:  IEAVIFAM                                              00850000
*                                                                       00900000
*01* EXTERNAL CLASSIFICATION: NONE                                      00950000
*01* END OF EXTERNAL CLASSIFICATION:                                    01000000
*                                                                       01050000
*01* DSECT NAME:                                                        01100000
*                                                                       01150000
*01* COMPONENT:  Supervisor Control (SC1C5)                             01200000
*                                                                       01250000
*01* EYE-CATCHER:  NONE                                                 01300000
*                                                                       01350000
*01* STORAGE ATTRIBUTES:                                                01400000
*02*  SUBPOOL:  N/A                                                     01450000
*02*  KEY:  N/A                                                         01500000
*02*  RESIDENCY:  N/A                                                   01550000
*                                                                       01600000
*01* SIZE:  N/A                                                         01650000
*                                                                       01700000
*01* CREATED BY:                                                        01750000
*      N/A                                                              01800000
*                                                                       01850000
*01* POINTED TO BY:                                                     01900000
*     N/A                                                               01950000
*                                                                       02000000
*01* SERIALIZATION:                                                     02050000
*     None required                                                     02100000
*                                                                       02150000
*01* FUNCTION:                                                          02200000
*02* Produces interface stub for switch to/from an IFA engine.          02250000
*                                                                       02300000
*01* METHOD OF ACCESS:                                                  02350000
*02*  ASM:                                                              02400000
*           SYSSTATE AMODE64={NO|YES}                                   02450000
*      name IEAVIFAM SWITCH={TO|FROM},C={YES|NO}(                       02471400
*     Default: SWITCH=TO,C=YES                                          02492800
*     C=NO is intended for testers who do not want to have a            02514200
*     C environment in order to use the service, but understand         02535600
*     the register-linkage conventions that the service adheres         02557000
*     to.                                                               02578400
*                                                                       02600000
*01* DELETED BY:  N/A                                                   02650000
*                                                                       02700000
*01* FREQUENCY:  N/A                                                    02750000
*                                                                       02800000
*01* DEPENDENCIES:  None                                                02850000
*                                                                       02900000
*01* DISTRIBUTION LIBRARY:  AINTLIB                                     02950000
*                                                                       03000000
*01* CHANGE ACTIVITY:                                                   03050000
*   $H0=IFA      HBB7709 031205 PD00XB: IFA support                     03100000
*                                                                       03150000
* END OF SPECIFICATIONS *********************************************/  03200000
         MACRO                                                          03250000
&NAME    IEAVIFAM &SWITCH=,&C=YES                                       03300000
         GBLC  &SYSAM64                                                 03350000
         LCLC  &MACPRFX                                                 03400000
         LCLC  &L,&LR                                                   03450000
         SYSSTATE TEST                                                  03500000
         AIF   ('&SYSAM64' EQ 'YES').AM64                               03550000
&MACPRFX SETC  'EDCX'                                                   03600000
&L       SETC  'L'                                                      03650000
&LR      SETC  'LR'                                                     03700000
         AGO   .AM3164                                                  03750000
.AM64    ANOP                                                           03800000
&MACPRFX SETC  'CELQ'                                                   03850000
&L       SETC  'LLGT'                                                   03900000
&LR      SETC  'LGR'                                                    03950000
.AM3164  ANOP                                                           04000000
         AIF   ('&C' NE 'YES').NOT_C_1                                  04025000
&NAME    &MACPRFX.PRLG DSASIZE=0,BASEREG=NONE                           04050000
.NOT_C_1 ANOP                                                           04075000
         &L    3,X'10'               Get CVT address                    04100000
         AIF   ('&SWITCH' EQ 'FROM').SWITCH_1A                          04150000
         LA    0,IEAVIFA_TO_IFA      Function code to switch to IFA     04200000
         AGO   .SWITCH_1B                                               04250000
.SWITCH_1A ANOP                                                         04300000
         LA    0,IEAVIFA_FROM_IFA    Function code to switch from IFA   04350000
.SWITCH_1B ANOP                                                         04400000
         TM    CVTOSLV4-CVTMAP(3),CVTIFAR    Make sure IFA routine      04450000
         JZ    NOT_AVAIL                                                04500000
         &L    3,CVTXSFT-CVTMAP(3)   Get address of SFT (pc # table)    04550000
         &LR   2,15                  Preserve R15 across PC/PR          04600000
         L     3,4*143-4(3,0) ENTRY 143  Get PC# for switch service     04650000
         PC    0(3)                  Call the switch service            04700000
         &LR   3,15                  Move RC to R3 for XPLink RC        04750000
         &LR   15,2                  Restore callers R15                04800000
         J     RETURN_TO_CALLER                                         04850000
NOT_AVAIL DS   0H                                                       04900000
         LA    3,IEAVIFA_NOTAVAILABLE   Set RC for wrong release        04950000
RETURN_TO_CALLER DS 0H                                                  05000000
         AIF   ('&C' NE 'YES').NOT_C_2                                  05025000
         &MACPRFX.EPLG                                                  05050000
.NOT_C_2 ANOP                                                           05075000
         CVT   DSECT=YES                                                05100000
         IEAVIFAI ,                                                     05150000
         AIF   ('&C' NE 'YES').NOT_C_3                                  05175000
         END                                                            05200000
.NOT_C_3 ANOP                                                           05225000
         MEND                                                           05250000

         AIF ('&SYSPARM' NE 'BIT64').JMP1
         SYSSTATE AMODE64=YES
.JMP1    ANOP
IEAVIFAT IEAVIFAM SWITCH=TO
         END
