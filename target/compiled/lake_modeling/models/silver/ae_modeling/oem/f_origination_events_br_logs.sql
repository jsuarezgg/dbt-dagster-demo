
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
acceptloanmaxattemptsreachedbr_br AS ( 
    SELECT *
    FROM bronze.acceptloanmaxattemptsreachedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,additionalinformationupdatedbr_br AS ( 
    SELECT *
    FROM bronze.additionalinformationupdatedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,allyisdisabledtooriginatebr_br AS ( 
    SELECT *
    FROM bronze.allyisdisabledtooriginatebr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationapproved_br AS ( 
    SELECT *
    FROM bronze.applicationapproved_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationcreated_br AS ( 
    SELECT *
    FROM bronze.applicationcreated_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationdeclined_br AS ( 
    SELECT *
    FROM bronze.applicationdeclined_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationdeviceinformationupdated_br AS ( 
    SELECT *
    FROM bronze.applicationdeviceinformationupdated_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationdeviceupdated_br AS ( 
    SELECT *
    FROM bronze.applicationdeviceupdated_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationexpired_br AS ( 
    SELECT *
    FROM bronze.applicationexpired_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationrestarted_br AS ( 
    SELECT *
    FROM bronze.applicationrestarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,backgroundcheckmaxattemptsreachedbr_br AS ( 
    SELECT *
    FROM bronze.backgroundcheckmaxattemptsreachedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,backgroundcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.backgroundcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,bankinglicensepartnercommunicationfailedbr_br AS ( 
    SELECT *
    FROM bronze.bankinglicensepartnercommunicationfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,basicidentityvalidatedbr_br AS ( 
    SELECT *
    FROM bronze.basicidentityvalidatedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,basicidverificationfailedbr_br AS ( 
    SELECT *
    FROM bronze.basicidverificationfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,bioinfodidnotmatchbr_br AS ( 
    SELECT *
    FROM bronze.bioinfodidnotmatchbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,carddownpaymentfailedbr_br AS ( 
    SELECT *
    FROM bronze.carddownpaymentfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,carddownpaymentgeneratedbr_br AS ( 
    SELECT *
    FROM bronze.carddownpaymentgeneratedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,carddownpaymentpaidbr_br AS ( 
    SELECT *
    FROM bronze.carddownpaymentpaidbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonealreadylinkedtoadifferentprospectbr_br AS ( 
    SELECT *
    FROM bronze.cellphonealreadylinkedtoadifferentprospectbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonealreadylinkedtoexistingclientbr_br AS ( 
    SELECT *
    FROM bronze.cellphonealreadylinkedtoexistingclientbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonelistedinblacklistbr_br AS ( 
    SELECT *
    FROM bronze.cellphonelistedinblacklistbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidatedbr_br AS ( 
    SELECT *
    FROM bronze.cellphonevalidatedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidationcodedidnotmatchbr_br AS ( 
    SELECT *
    FROM bronze.cellphonevalidationcodedidnotmatchbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidationcodewasexpiredbr_br AS ( 
    SELECT *
    FROM bronze.cellphonevalidationcodewasexpiredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidationmaxattemptsreachedbr_br AS ( 
    SELECT *
    FROM bronze.cellphonevalidationmaxattemptsreachedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidationnotificationsentbr_br AS ( 
    SELECT *
    FROM bronze.cellphonevalidationnotificationsentbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginacceptancecodedidnotmatchbr_br AS ( 
    SELECT *
    FROM bronze.checkoutloginacceptancecodedidnotmatchbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginacceptancewasexpiredbr_br AS ( 
    SELECT *
    FROM bronze.checkoutloginacceptancewasexpiredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginacceptedbr_br AS ( 
    SELECT *
    FROM bronze.checkoutloginacceptedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginsentbr_br AS ( 
    SELECT *
    FROM bronze.checkoutloginsentbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkouttransactioncompletedbr_br AS ( 
    SELECT *
    FROM bronze.checkouttransactioncompletedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkouttransactionstartedbr_br AS ( 
    SELECT *
    FROM bronze.checkouttransactionstartedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientaddicupowasbalancezerobr_br AS ( 
    SELECT *
    FROM bronze.clientaddicupowasbalancezerobr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientautopaymenttokenizedcardstoredbr_br AS ( 
    SELECT *
    FROM bronze.clientautopaymenttokenizedcardstoredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailedbr_br AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientfailedtoobtainscoringbr_br AS ( 
    SELECT *
    FROM bronze.clientfailedtoobtainscoringbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientfraudcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.clientfraudcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientloanproposalswerewithinvalidusuryratebr_br AS ( 
    SELECT *
    FROM bronze.clientloanproposalswerewithinvalidusuryratebr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentbehaviorvalidationapprovedbr_br AS ( 
    SELECT *
    FROM bronze.clientpaymentbehaviorvalidationapprovedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentbehaviorvalidationrejectedbr_br AS ( 
    SELECT *
    FROM bronze.clientpaymentbehaviorvalidationrejectedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpreapprovaljourneyisdisabledbr_br AS ( 
    SELECT *
    FROM bronze.clientpreapprovaljourneyisdisabledbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientwaspreapprovedbeforebr_br AS ( 
    SELECT *
    FROM bronze.clientwaspreapprovedbeforebr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedwithlowerapprovedamountbr_br AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedwithlowerapprovedamountbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedbr_br AS ( 
    SELECT *
    FROM bronze.creditcheckfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.creditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedwithlowerapprovedamountbr_br AS ( 
    SELECT *
    FROM bronze.creditcheckpassedwithlowerapprovedamountbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,documentlistedinblacklistbr_br AS ( 
    SELECT *
    FROM bronze.documentlistedinblacklistbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,emailalreadylinkedtoexistingclientbr_br AS ( 
    SELECT *
    FROM bronze.emailalreadylinkedtoexistingclientbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,emaillistedinblacklistbr_br AS ( 
    SELECT *
    FROM bronze.emaillistedinblacklistbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,expeditedcreditcardaddedbr_br AS ( 
    SELECT *
    FROM bronze.expeditedcreditcardaddedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,expeditedcreditcardchargedbr_br AS ( 
    SELECT *
    FROM bronze.expeditedcreditcardchargedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,expeditedcreditcardchargefailedbr_br AS ( 
    SELECT *
    FROM bronze.expeditedcreditcardchargefailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,expeditedcreditcardpaymentgeneratedbr_br AS ( 
    SELECT *
    FROM bronze.expeditedcreditcardpaymentgeneratedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,expeditedcreditcardselectedbr_br AS ( 
    SELECT *
    FROM bronze.expeditedcreditcardselectedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,expeditedcreditcardsfoundbr_br AS ( 
    SELECT *
    FROM bronze.expeditedcreditcardsfoundbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,expeditednocreditcardfoundbr_br AS ( 
    SELECT *
    FROM bronze.expeditednocreditcardfoundbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtoobtaincommercialinformationbr_br AS ( 
    SELECT *
    FROM bronze.failedtoobtaincommercialinformationbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtoobtainscoringbr_br AS ( 
    SELECT *
    FROM bronze.failedtoobtainscoringbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,fraudcheckfailedbr_br AS ( 
    SELECT *
    FROM bronze.fraudcheckfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,fraudcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.fraudcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosagentassigned_br AS ( 
    SELECT *
    FROM bronze.identityphotosagentassigned_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosapproved_br AS ( 
    SELECT *
    FROM bronze.identityphotosapproved_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotoscollected_br AS ( 
    SELECT *
    FROM bronze.identityphotoscollected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosdiscarded_br AS ( 
    SELECT *
    FROM bronze.identityphotosdiscarded_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosdiscardedbyrisk_br AS ( 
    SELECT *
    FROM bronze.identityphotosdiscardedbyrisk_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosevaluationstarted_br AS ( 
    SELECT *
    FROM bronze.identityphotosevaluationstarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotoskeptbyagent_br AS ( 
    SELECT *
    FROM bronze.identityphotoskeptbyagent_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosrejected_br AS ( 
    SELECT *
    FROM bronze.identityphotosrejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosstarted_br AS ( 
    SELECT *
    FROM bronze.identityphotosstarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywaagentassigned_br AS ( 
    SELECT *
    FROM bronze.identitywaagentassigned_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywaapproved_br AS ( 
    SELECT *
    FROM bronze.identitywaapproved_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywadiscarded_br AS ( 
    SELECT *
    FROM bronze.identitywadiscarded_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywadiscardedbyrisk_br AS ( 
    SELECT *
    FROM bronze.identitywadiscardedbyrisk_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywainitialmessageresponsereceived_br AS ( 
    SELECT *
    FROM bronze.identitywainitialmessageresponsereceived_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywainputinformationcompleted_br AS ( 
    SELECT *
    FROM bronze.identitywainputinformationcompleted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywakeptbyagent_br AS ( 
    SELECT *
    FROM bronze.identitywakeptbyagent_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywarejected_br AS ( 
    SELECT *
    FROM bronze.identitywarejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywastarted_br AS ( 
    SELECT *
    FROM bronze.identitywastarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyapproved_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyapproved_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartycollected_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartycollected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartymanualverificationrequired_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartymanualverificationrequired_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyphotorejected_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyphotorejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyrejected_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyrejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyrequested_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyrequested_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartyskipped_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartyskipped_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,idvthirdpartystarted_br AS ( 
    SELECT *
    FROM bronze.idvthirdpartystarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,invalidemailbr_br AS ( 
    SELECT *
    FROM bronze.invalidemailbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancecodedidnotmatchbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptancecodedidnotmatchbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancenotificationfailedbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptancenotificationfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancesentbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptancesentbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancewasexpiredbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptancewasexpiredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedbybankinglicensepartnerbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptedbybankinglicensepartnerbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedwasnotsenttobankinglicensepartnerbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptedwasnotsenttobankinglicensepartnerbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposaldownpaymentselectedbr_br AS ( 
    SELECT *
    FROM bronze.loanproposaldownpaymentselectedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalselectedbr_br AS ( 
    SELECT *
    FROM bronze.loanproposalselectedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalswithinvalidusuryratesbr_br AS ( 
    SELECT *
    FROM bronze.loanproposalswithinvalidusuryratesbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanrejectedbybankinglicensepartnerbr_br AS ( 
    SELECT *
    FROM bronze.loanrejectedbybankinglicensepartnerbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,originationunexpectederroroccurred_br AS ( 
    SELECT *
    FROM bronze.originationunexpectederroroccurred_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pendingdownpaymentnotifiedbr_br AS ( 
    SELECT *
    FROM bronze.pendingdownpaymentnotifiedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pendingloanproposalnotifiedbr_br AS ( 
    SELECT *
    FROM bronze.pendingloanproposalnotifiedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,personalinformationupdatedbr_br AS ( 
    SELECT *
    FROM bronze.personalinformationupdatedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pixdownpaymentgeneratedbr_br AS ( 
    SELECT *
    FROM bronze.pixdownpaymentgeneratedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pixdownpaymentpaidbr_br AS ( 
    SELECT *
    FROM bronze.pixdownpaymentpaidbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pixdownpaymentpaidbydifferentpersonbr_br AS ( 
    SELECT *
    FROM bronze.pixdownpaymentpaidbydifferentpersonbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pixdownpaymentpaidthroughblacklistedbankbr_br AS ( 
    SELECT *
    FROM bronze.pixdownpaymentpaidthroughblacklistedbankbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pixpaymentgeneratedbr_br AS ( 
    SELECT *
    FROM bronze.pixpaymentgeneratedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pixpaymentgenerationfailedbr_br AS ( 
    SELECT *
    FROM bronze.pixpaymentgenerationfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pixpaymentreceivedbr_br AS ( 
    SELECT *
    FROM bronze.pixpaymentreceivedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pixpaymentrequestedbr_br AS ( 
    SELECT *
    FROM bronze.pixpaymentrequestedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalapplicationcompletedbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalapplicationcompletedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalconditionalcreditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalconditionalcreditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckfailedbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalfailedtoobtaincommercialinformationbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalfailedtoobtaincommercialinformationbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalfailedtoobtainscoringbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalfailedtoobtainscoringbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preconditionswerevalidbr_br AS ( 
    SELECT *
    FROM bronze.preconditionswerevalidbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyacceptedbr_br AS ( 
    SELECT *
    FROM bronze.privacypolicyacceptedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectalreadylinkedtodifferentcellphonebr_br AS ( 
    SELECT *
    FROM bronze.prospectalreadylinkedtodifferentcellphonebr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectdocumentnotfoundinbureaubr_br AS ( 
    SELECT *
    FROM bronze.prospectdocumentnotfoundinbureaubr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectinfonotcompleteinbureaubr_br AS ( 
    SELECT *
    FROM bronze.prospectinfonotcompleteinbureaubr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationfailed_br AS ( 
    SELECT *
    FROM bronze.psychometricevaluationfailed_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationisrequiredbr_br AS ( 
    SELECT *
    FROM bronze.psychometricevaluationisrequiredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationrated_br AS ( 
    SELECT *
    FROM bronze.psychometricevaluationrated_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationstarted_br AS ( 
    SELECT *
    FROM bronze.psychometricevaluationstarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwasgreaterthanmaximumconfiguredbr_br AS ( 
    SELECT *
    FROM bronze.requestedamountwasgreaterthanmaximumconfiguredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwaslessthanminimumconfiguredbr_br AS ( 
    SELECT *
    FROM bronze.requestedamountwaslessthanminimumconfiguredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,sendcellphonevalidationnotificationmaxattemptsreachedbr_br AS ( 
    SELECT *
    FROM bronze.sendcellphonevalidationnotificationmaxattemptsreachedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,sendloanacceptancemaxattemptsreachedbr_br AS ( 
    SELECT *
    FROM bronze.sendloanacceptancemaxattemptsreachedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,subproductselectedbr_br AS ( 
    SELECT *
    FROM bronze.subproductselectedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pendingpaymentbnpnnotifiedbr_br AS ( 
    SELECT *
    FROM bronze.pendingpaymentbnpnnotifiedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,counterofferacceptedbr_br AS ( 
    SELECT *
    FROM bronze.counterofferacceptedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,counterofferdeclinedbr_br AS ( 
    SELECT *
    FROM bronze.counterofferdeclinedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,counterofferfoundbr_br AS ( 
    SELECT *
    FROM bronze.counterofferfoundbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,counterofferrequestedbr_br AS ( 
    SELECT *
    FROM bronze.counterofferrequestedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcounterofferrequestedbr_br AS ( 
    SELECT *
    FROM bronze.clientcounterofferrequestedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedwithoutloan_br AS ( 
    SELECT *
    FROM bronze.creditcheckpassedwithoutloan_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM acceptloanmaxattemptsreachedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM additionalinformationupdatedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM allyisdisabledtooriginatebr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationapproved_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationcreated_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationdeclined_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationdeviceinformationupdated_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationdeviceupdated_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationexpired_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationrestarted_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM backgroundcheckmaxattemptsreachedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM backgroundcheckpassedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM bankinglicensepartnercommunicationfailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM basicidentityvalidatedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM basicidverificationfailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM bioinfodidnotmatchbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM carddownpaymentfailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM carddownpaymentgeneratedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM carddownpaymentpaidbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonealreadylinkedtoadifferentprospectbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonealreadylinkedtoexistingclientbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonelistedinblacklistbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidatedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidationcodedidnotmatchbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidationcodewasexpiredbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidationmaxattemptsreachedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidationnotificationsentbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkoutloginacceptancecodedidnotmatchbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkoutloginacceptancewasexpiredbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkoutloginacceptedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkoutloginsentbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkouttransactioncompletedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkouttransactionstartedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientaddicupowasbalancezerobr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientautopaymenttokenizedcardstoredbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientcreditcheckfailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientfailedtoobtainscoringbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientfraudcheckpassedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientloanproposalswerewithinvalidusuryratebr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpaymentbehaviorvalidationapprovedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpaymentbehaviorvalidationrejectedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpreapprovaljourneyisdisabledbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientwaspreapprovedbeforebr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM conditionalcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM conditionalcreditcheckpassedwithlowerapprovedamountbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckfailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckpassedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckpassedwithlowerapprovedamountbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM documentlistedinblacklistbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM emailalreadylinkedtoexistingclientbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM emaillistedinblacklistbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM expeditedcreditcardaddedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM expeditedcreditcardchargedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM expeditedcreditcardchargefailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM expeditedcreditcardpaymentgeneratedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM expeditedcreditcardselectedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM expeditedcreditcardsfoundbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM expeditednocreditcardfoundbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM failedtoobtaincommercialinformationbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM failedtoobtainscoringbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM fraudcheckfailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM fraudcheckpassedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosagentassigned_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosapproved_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotoscollected_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosdiscarded_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosdiscardedbyrisk_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosevaluationstarted_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotoskeptbyagent_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosrejected_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosstarted_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywaagentassigned_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywaapproved_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywadiscarded_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywadiscardedbyrisk_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywainitialmessageresponsereceived_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywainputinformationcompleted_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywakeptbyagent_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywarejected_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywastarted_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM idvthirdpartyapproved_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM idvthirdpartycollected_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM idvthirdpartymanualverificationrequired_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM idvthirdpartyphotorejected_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM idvthirdpartyrejected_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM idvthirdpartyrequested_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM idvthirdpartyskipped_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM idvthirdpartystarted_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM invalidemailbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancecodedidnotmatchbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancenotificationfailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancesentbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancewasexpiredbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptedbybankinglicensepartnerbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptedwasnotsenttobankinglicensepartnerbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanproposaldownpaymentselectedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanproposalselectedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanproposalswithinvalidusuryratesbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanrejectedbybankinglicensepartnerbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM originationunexpectederroroccurred_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pendingdownpaymentnotifiedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pendingloanproposalnotifiedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM personalinformationupdatedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pixdownpaymentgeneratedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pixdownpaymentpaidbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pixdownpaymentpaidbydifferentpersonbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pixdownpaymentpaidthroughblacklistedbankbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pixpaymentgeneratedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pixpaymentgenerationfailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pixpaymentreceivedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pixpaymentrequestedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalapplicationcompletedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalconditionalcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalcreditcheckfailedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalfailedtoobtaincommercialinformationbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalfailedtoobtainscoringbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preconditionswerevalidbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM privacypolicyacceptedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM prospectalreadylinkedtodifferentcellphonebr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM prospectdocumentnotfoundinbureaubr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM prospectinfonotcompleteinbureaubr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationfailed_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationisrequiredbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationrated_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationstarted_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM requestedamountwasgreaterthanmaximumconfiguredbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM requestedamountwaslessthanminimumconfiguredbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM sendcellphonevalidationnotificationmaxattemptsreachedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM sendloanacceptancemaxattemptsreachedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM subproductselectedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM pendingpaymentbnpnnotifiedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM counterofferacceptedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM counterofferdeclinedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM counterofferfoundbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM counterofferrequestedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientcounterofferrequestedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckpassedwithoutloan_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM union_bronze 
    
)   



, final AS (
    SELECT 
        *,
        date(ocurred_on ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM union_all_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_origination_events_br_logs
country: br
silver_table_name: f_origination_events_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'application_id', 'channel', 'client_id', 'client_type', 'event_name', 'event_type', 'journey_name', 'journey_stage_name', 'ocurred_on', 'product']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'acceptloanmaxattemptsreachedbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 0, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'additionalinformationupdatedbr': {'stage': 'additional_information_br', 'reference_order_id': 4, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'allyisdisabledtooriginatebr': {'stage': 'preconditions_br', 'reference_order_id': 5, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationapproved': {'stage': 'GLOBAL', 'reference_order_id': 9, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationcreated': {'stage': 'GLOBAL', 'reference_order_id': 10, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationdeclined': {'stage': 'GLOBAL', 'reference_order_id': 11, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationdeviceinformationupdated': {'stage': 'device_information', 'reference_order_id': 12, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationdeviceupdated': {'stage': 'GLOBAL', 'reference_order_id': 13, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationexpired': {'stage': 'GLOBAL', 'reference_order_id': 14, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationrestarted': {'stage': 'GLOBAL', 'reference_order_id': 15, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'backgroundcheckmaxattemptsreachedbr': {'stage': 'background_check_br', 'reference_order_id': 16, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'backgroundcheckpassedbr': {'stage': 'background_check_br', 'reference_order_id': 17, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'bankinglicensepartnercommunicationfailedbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 19, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'basicidentityvalidatedbr': {'stage': 'basic_identity_br', 'reference_order_id': 20, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'basicidverificationfailedbr': {'stage': 'basic_identity_br', 'reference_order_id': 22, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'bioinfodidnotmatchbr': {'stage': 'basic_identity_br', 'reference_order_id': 24, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'carddownpaymentfailedbr': {'stage': 'down_payments_br', 'reference_order_id': 26, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'carddownpaymentgeneratedbr': {'stage': 'down_payments_br', 'reference_order_id': 27, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'carddownpaymentpaidbr': {'stage': 'down_payments_br', 'reference_order_id': 28, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonealreadylinkedtoadifferentprospectbr': {'stage': 'background_check_br', 'reference_order_id': 30, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonealreadylinkedtoexistingclientbr': {'stage': 'background_check_br', 'reference_order_id': 32, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonelistedinblacklistbr': {'stage': 'background_check_br', 'reference_order_id': 34, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidatedbr': {'stage': 'cellphone_validation_br', 'reference_order_id': 36, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidationcodedidnotmatchbr': {'stage': 'cellphone_validation_br', 'reference_order_id': 37, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidationcodewasexpiredbr': {'stage': 'cellphone_validation_br', 'reference_order_id': 38, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidationmaxattemptsreachedbr': {'stage': 'cellphone_validation_br', 'reference_order_id': 39, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidationnotificationsentbr': {'stage': 'cellphone_validation_br', 'reference_order_id': 40, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkoutloginacceptancecodedidnotmatchbr': {'stage': 'expedited_checkout_login_br', 'reference_order_id': 41, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkoutloginacceptancewasexpiredbr': {'stage': 'expedited_checkout_login_br', 'reference_order_id': 44, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkoutloginacceptedbr': {'stage': 'expedited_checkout_login_br', 'reference_order_id': 46, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkoutloginsentbr': {'stage': 'expedited_checkout_login_br', 'reference_order_id': 48, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkouttransactioncompletedbr': {'stage': 'expedited_checkout_transaction_br', 'reference_order_id': 50, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkouttransactionstartedbr': {'stage': 'expedited_checkout_transaction_br', 'reference_order_id': 52, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientaddicupowasbalancezerobr': {'stage': 'underwriting_rc_br', 'reference_order_id': 54, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientautopaymenttokenizedcardstoredbr': {'stage': 'loan_proposals_br', 'reference_order_id': 57, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientcreditcheckfailedbr': {'stage': 'underwriting_rc_br', 'reference_order_id': 58, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientcreditcheckpassedbr': {'stage': 'underwriting_rc_br', 'reference_order_id': 61, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientfailedtoobtainscoringbr': {'stage': 'underwriting_rc_br', 'reference_order_id': 65, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientfraudcheckpassedbr': {'stage': 'fraud_check_rc_br', 'reference_order_id': 70, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientloanproposalswerewithinvalidusuryratebr': {'stage': 'underwriting_rc_br', 'reference_order_id': 74, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpaymentbehaviorvalidationapprovedbr': {'stage': 'underwriting_rc_br', 'reference_order_id': 76, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpaymentbehaviorvalidationrejectedbr': {'stage': 'underwriting_rc_br', 'reference_order_id': 79, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpreapprovaljourneyisdisabledbr': {'stage': 'preconditions_br', 'reference_order_id': 82, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientwaspreapprovedbeforebr': {'stage': 'preconditions_br', 'reference_order_id': 84, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'conditionalcreditcheckpassedbr': {'stage': 'underwriting_br', 'reference_order_id': 86, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'conditionalcreditcheckpassedwithlowerapprovedamountbr': {'stage': 'underwriting_br', 'reference_order_id': 89, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckfailedbr': {'stage': 'underwriting_br', 'reference_order_id': 93, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckpassedbr': {'stage': 'underwriting_br', 'reference_order_id': 97, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckpassedwithlowerapprovedamountbr': {'stage': 'underwriting_br', 'reference_order_id': 100, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'documentlistedinblacklistbr': {'stage': 'background_check_br', 'reference_order_id': 104, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'emailalreadylinkedtoexistingclientbr': {'stage': 'background_check_br', 'reference_order_id': 107, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'emaillistedinblacklistbr': {'stage': 'background_check_br', 'reference_order_id': 109, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'expeditedcreditcardaddedbr': {'stage': 'expedited_add_credit_card_br', 'reference_order_id': 112, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'expeditedcreditcardchargedbr': {'stage': 'expedited_credit_card_charge_payment_br', 'reference_order_id': 113, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'expeditedcreditcardchargefailedbr': {'stage': 'expedited_credit_card_charge_payment_br', 'reference_order_id': 114, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'expeditedcreditcardpaymentgeneratedbr': {'stage': 'expedited_credit_card_charge_payment_br', 'reference_order_id': 115, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'expeditedcreditcardselectedbr': {'stage': 'expedited_select_credit_card_br', 'reference_order_id': 116, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'expeditedcreditcardsfoundbr': {'stage': 'expedited_get_credit_cards_br', 'reference_order_id': 117, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'expeditednocreditcardfoundbr': {'stage': 'expedited_get_credit_cards_br', 'reference_order_id': 118, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'failedtoobtaincommercialinformationbr': {'stage': 'underwriting_br', 'reference_order_id': 120, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'failedtoobtainscoringbr': {'stage': 'underwriting_br', 'reference_order_id': 122, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'fraudcheckfailedbr': {'stage': 'fraud_check_br', 'reference_order_id': 129, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'fraudcheckpassedbr': {'stage': 'fraud_check_br', 'reference_order_id': 131, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosagentassigned': {'stage': 'identity_photos', 'reference_order_id': 135, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosapproved': {'stage': 'identity_photos', 'reference_order_id': 136, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotoscollected': {'stage': 'identity_photos', 'reference_order_id': 137, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosdiscarded': {'stage': 'identity_photos', 'reference_order_id': 138, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosdiscardedbyrisk': {'stage': 'identity_photos', 'reference_order_id': 139, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosevaluationstarted': {'stage': 'identity_photos', 'reference_order_id': 140, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotoskeptbyagent': {'stage': 'identity_photos', 'reference_order_id': 141, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosrejected': {'stage': 'identity_photos', 'reference_order_id': 142, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosstarted': {'stage': 'identity_photos', 'reference_order_id': 143, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywaagentassigned': {'stage': 'identity_wa', 'reference_order_id': 151, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywaapproved': {'stage': 'identity_wa', 'reference_order_id': 152, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywadiscarded': {'stage': 'identity_wa', 'reference_order_id': 153, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywadiscardedbyrisk': {'stage': 'identity_wa', 'reference_order_id': 154, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywainitialmessageresponsereceived': {'stage': 'identity_wa', 'reference_order_id': 155, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywainputinformationcompleted': {'stage': 'identity_wa', 'reference_order_id': 156, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywakeptbyagent': {'stage': 'identity_wa', 'reference_order_id': 157, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywarejected': {'stage': 'identity_wa', 'reference_order_id': 158, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywastarted': {'stage': 'identity_wa', 'reference_order_id': 159, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'idvthirdpartyapproved': {'stage': 'idv_third_party', 'reference_order_id': 160, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'idvthirdpartycollected': {'stage': 'idv_third_party', 'reference_order_id': 161, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'idvthirdpartymanualverificationrequired': {'stage': 'idv_third_party', 'reference_order_id': 162, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'idvthirdpartyphotorejected': {'stage': 'idv_third_party', 'reference_order_id': 163, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'idvthirdpartyrejected': {'stage': 'idv_third_party', 'reference_order_id': 164, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'idvthirdpartyrequested': {'stage': 'idv_third_party', 'reference_order_id': 165, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'idvthirdpartyskipped': {'stage': 'idv_third_party', 'reference_order_id': 166, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'idvthirdpartystarted': {'stage': 'idv_third_party', 'reference_order_id': 167, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'invalidemailbr': {'stage': 'background_check_br', 'reference_order_id': 168, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancecodedidnotmatchbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 173, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancenotificationfailedbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 176, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancesentbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 179, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancewasexpiredbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 182, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptedbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 185, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptedbybankinglicensepartnerbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 186, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptedwasnotsenttobankinglicensepartnerbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 190, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanproposaldownpaymentselectedbr': {'stage': 'loan_proposals_down_payment_br', 'reference_order_id': 191, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanproposalselectedbr': {'stage': 'loan_proposals_br', 'reference_order_id': 192, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanproposalswithinvalidusuryratesbr': {'stage': 'underwriting_br', 'reference_order_id': 195, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanrejectedbybankinglicensepartnerbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 196, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'originationunexpectederroroccurred': {'stage': 'GLOBAL', 'reference_order_id': 197, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pendingdownpaymentnotifiedbr': {'stage': 'down_payments_br', 'reference_order_id': 198, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pendingloanproposalnotifiedbr': {'stage': 'loan_proposals_br', 'reference_order_id': 199, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'personalinformationupdatedbr': {'stage': 'personal_information_br', 'reference_order_id': 200, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pixdownpaymentgeneratedbr': {'stage': 'down_payments_br', 'reference_order_id': 201, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pixdownpaymentpaidbr': {'stage': 'down_payments_br', 'reference_order_id': 202, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pixdownpaymentpaidbydifferentpersonbr': {'stage': 'down_payments_br', 'reference_order_id': 203, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pixdownpaymentpaidthroughblacklistedbankbr': {'stage': 'down_payments_br', 'reference_order_id': 204, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pixpaymentgeneratedbr': {'stage': 'bn_pn_payments_br', 'reference_order_id': 205, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pixpaymentgenerationfailedbr': {'stage': 'bn_pn_payments_br', 'reference_order_id': 206, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pixpaymentreceivedbr': {'stage': 'bn_pn_payments_br', 'reference_order_id': 207, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pixpaymentrequestedbr': {'stage': 'bn_pn_payments_br', 'reference_order_id': 208, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalapplicationcompletedbr': {'stage': 'preapproval_summary_br', 'reference_order_id': 209, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalconditionalcreditcheckpassedbr': {'stage': 'underwriting_preapproval_br', 'reference_order_id': 212, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalcreditcheckfailedbr': {'stage': 'underwriting_preapproval_br', 'reference_order_id': 215, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalcreditcheckpassedbr': {'stage': 'underwriting_preapproval_br', 'reference_order_id': 218, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalfailedtoobtaincommercialinformationbr': {'stage': 'underwriting_preapproval_br', 'reference_order_id': 222, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalfailedtoobtainscoringbr': {'stage': 'underwriting_preapproval_br', 'reference_order_id': 224, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preconditionswerevalidbr': {'stage': 'preconditions_br', 'reference_order_id': 229, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'privacypolicyacceptedbr': {'stage': 'privacy_policy_br', 'reference_order_id': 232, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'prospectalreadylinkedtodifferentcellphonebr': {'stage': 'background_check_br', 'reference_order_id': 235, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'prospectdocumentnotfoundinbureaubr': {'stage': 'basic_identity_br', 'reference_order_id': 237, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'prospectinfonotcompleteinbureaubr': {'stage': 'basic_identity_br', 'reference_order_id': 239, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationfailed': {'stage': 'psychometric_assessment', 'reference_order_id': 242, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationisrequiredbr': {'stage': 'underwriting_br', 'reference_order_id': 243, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationrated': {'stage': 'psychometric_assessment', 'reference_order_id': 246, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationstarted': {'stage': 'psychometric_assessment', 'reference_order_id': 248, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'requestedamountwasgreaterthanmaximumconfiguredbr': {'stage': 'preconditions_br', 'reference_order_id': 249, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'requestedamountwaslessthanminimumconfiguredbr': {'stage': 'preconditions_br', 'reference_order_id': 252, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'sendcellphonevalidationnotificationmaxattemptsreachedbr': {'stage': 'cellphone_validation_br', 'reference_order_id': 255, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'sendloanacceptancemaxattemptsreachedbr': {'stage': 'loan_acceptance_br', 'reference_order_id': 256, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'subproductselectedbr': {'stage': 'subproduct_selection_br', 'reference_order_id': 260, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'pendingpaymentbnpnnotifiedbr': {'stage': 'bn_pn_payments_br', 'reference_order_id': 261, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'counterofferacceptedbr': {'stage': 'counter_offer_br', 'reference_order_id': 262, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'counterofferdeclinedbr': {'stage': 'counter_offer_br', 'reference_order_id': 263, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'counterofferfoundbr': {'stage': 'counter_offer_br', 'reference_order_id': 264, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'counterofferrequestedbr': {'stage': 'underwriting_br', 'reference_order_id': 265, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientcounterofferrequestedbr': {'stage': 'underwriting_rc_br', 'reference_order_id': 266, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckpassedwithoutloan': {'stage': 'underwriting', 'reference_order_id': 268, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}}
events_keys: ['acceptloanmaxattemptsreachedbr', 'additionalinformationupdatedbr', 'allyisdisabledtooriginatebr', 'applicationapproved', 'applicationcreated', 'applicationdeclined', 'applicationdeviceinformationupdated', 'applicationdeviceupdated', 'applicationexpired', 'applicationrestarted', 'backgroundcheckmaxattemptsreachedbr', 'backgroundcheckpassedbr', 'bankinglicensepartnercommunicationfailedbr', 'basicidentityvalidatedbr', 'basicidverificationfailedbr', 'bioinfodidnotmatchbr', 'carddownpaymentfailedbr', 'carddownpaymentgeneratedbr', 'carddownpaymentpaidbr', 'cellphonealreadylinkedtoadifferentprospectbr', 'cellphonealreadylinkedtoexistingclientbr', 'cellphonelistedinblacklistbr', 'cellphonevalidatedbr', 'cellphonevalidationcodedidnotmatchbr', 'cellphonevalidationcodewasexpiredbr', 'cellphonevalidationmaxattemptsreachedbr', 'cellphonevalidationnotificationsentbr', 'checkoutloginacceptancecodedidnotmatchbr', 'checkoutloginacceptancewasexpiredbr', 'checkoutloginacceptedbr', 'checkoutloginsentbr', 'checkouttransactioncompletedbr', 'checkouttransactionstartedbr', 'clientaddicupowasbalancezerobr', 'clientautopaymenttokenizedcardstoredbr', 'clientcreditcheckfailedbr', 'clientcreditcheckpassedbr', 'clientfailedtoobtainscoringbr', 'clientfraudcheckpassedbr', 'clientloanproposalswerewithinvalidusuryratebr', 'clientpaymentbehaviorvalidationapprovedbr', 'clientpaymentbehaviorvalidationrejectedbr', 'clientpreapprovaljourneyisdisabledbr', 'clientwaspreapprovedbeforebr', 'conditionalcreditcheckpassedbr', 'conditionalcreditcheckpassedwithlowerapprovedamountbr', 'creditcheckfailedbr', 'creditcheckpassedbr', 'creditcheckpassedwithlowerapprovedamountbr', 'documentlistedinblacklistbr', 'emailalreadylinkedtoexistingclientbr', 'emaillistedinblacklistbr', 'expeditedcreditcardaddedbr', 'expeditedcreditcardchargedbr', 'expeditedcreditcardchargefailedbr', 'expeditedcreditcardpaymentgeneratedbr', 'expeditedcreditcardselectedbr', 'expeditedcreditcardsfoundbr', 'expeditednocreditcardfoundbr', 'failedtoobtaincommercialinformationbr', 'failedtoobtainscoringbr', 'fraudcheckfailedbr', 'fraudcheckpassedbr', 'identityphotosagentassigned', 'identityphotosapproved', 'identityphotoscollected', 'identityphotosdiscarded', 'identityphotosdiscardedbyrisk', 'identityphotosevaluationstarted', 'identityphotoskeptbyagent', 'identityphotosrejected', 'identityphotosstarted', 'identitywaagentassigned', 'identitywaapproved', 'identitywadiscarded', 'identitywadiscardedbyrisk', 'identitywainitialmessageresponsereceived', 'identitywainputinformationcompleted', 'identitywakeptbyagent', 'identitywarejected', 'identitywastarted', 'idvthirdpartyapproved', 'idvthirdpartycollected', 'idvthirdpartymanualverificationrequired', 'idvthirdpartyphotorejected', 'idvthirdpartyrejected', 'idvthirdpartyrequested', 'idvthirdpartyskipped', 'idvthirdpartystarted', 'invalidemailbr', 'loanacceptancecodedidnotmatchbr', 'loanacceptancenotificationfailedbr', 'loanacceptancesentbr', 'loanacceptancewasexpiredbr', 'loanacceptedbr', 'loanacceptedbybankinglicensepartnerbr', 'loanacceptedwasnotsenttobankinglicensepartnerbr', 'loanproposaldownpaymentselectedbr', 'loanproposalselectedbr', 'loanproposalswithinvalidusuryratesbr', 'loanrejectedbybankinglicensepartnerbr', 'originationunexpectederroroccurred', 'pendingdownpaymentnotifiedbr', 'pendingloanproposalnotifiedbr', 'personalinformationupdatedbr', 'pixdownpaymentgeneratedbr', 'pixdownpaymentpaidbr', 'pixdownpaymentpaidbydifferentpersonbr', 'pixdownpaymentpaidthroughblacklistedbankbr', 'pixpaymentgeneratedbr', 'pixpaymentgenerationfailedbr', 'pixpaymentreceivedbr', 'pixpaymentrequestedbr', 'preapprovalapplicationcompletedbr', 'preapprovalconditionalcreditcheckpassedbr', 'preapprovalcreditcheckfailedbr', 'preapprovalcreditcheckpassedbr', 'preapprovalfailedtoobtaincommercialinformationbr', 'preapprovalfailedtoobtainscoringbr', 'preconditionswerevalidbr', 'privacypolicyacceptedbr', 'prospectalreadylinkedtodifferentcellphonebr', 'prospectdocumentnotfoundinbureaubr', 'prospectinfonotcompleteinbureaubr', 'psychometricevaluationfailed', 'psychometricevaluationisrequiredbr', 'psychometricevaluationrated', 'psychometricevaluationstarted', 'requestedamountwasgreaterthanmaximumconfiguredbr', 'requestedamountwaslessthanminimumconfiguredbr', 'sendcellphonevalidationnotificationmaxattemptsreachedbr', 'sendloanacceptancemaxattemptsreachedbr', 'subproductselectedbr', 'pendingpaymentbnpnnotifiedbr', 'counterofferacceptedbr', 'counterofferdeclinedbr', 'counterofferfoundbr', 'counterofferrequestedbr', 'clientcounterofferrequestedbr', 'creditcheckpassedwithoutloan']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
