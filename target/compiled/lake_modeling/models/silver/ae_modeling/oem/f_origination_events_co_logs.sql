
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
acceptloanmaxattemptsreachedco_co AS ( 
    SELECT *
    FROM bronze.acceptloanmaxattemptsreachedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,acceptloanmaxattemptsreachedsantanderco_co AS ( 
    SELECT *
    FROM bronze.acceptloanmaxattemptsreachedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,additionalinformationreceivedsantanderco_co AS ( 
    SELECT *
    FROM bronze.additionalinformationreceivedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,allyisdisabledtooriginateco_co AS ( 
    SELECT *
    FROM bronze.allyisdisabledtooriginateco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,allyisdisabledtooriginatepagoco_co AS ( 
    SELECT *
    FROM bronze.allyisdisabledtooriginatepagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationadjustedco_co AS ( 
    SELECT *
    FROM bronze.applicationadjustedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationcreated_co AS ( 
    SELECT *
    FROM bronze.applicationcreated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationdeclined_co AS ( 
    SELECT *
    FROM bronze.applicationdeclined_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationdeviceinformationupdated_co AS ( 
    SELECT *
    FROM bronze.applicationdeviceinformationupdated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationdeviceupdated_co AS ( 
    SELECT *
    FROM bronze.applicationdeviceupdated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationexpired_co AS ( 
    SELECT *
    FROM bronze.applicationexpired_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationrestarted_co AS ( 
    SELECT *
    FROM bronze.applicationrestarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,backgroundcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.backgroundcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,basicidentityvalidatedco_co AS ( 
    SELECT *
    FROM bronze.basicidentityvalidatedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,basicidverificationfailedco_co AS ( 
    SELECT *
    FROM bronze.basicidverificationfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,bioinfodidnotmatchco_co AS ( 
    SELECT *
    FROM bronze.bioinfodidnotmatchco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,casecreatedsantanderco_co AS ( 
    SELECT *
    FROM bronze.casecreatedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonealreadylinkedtoadifferentprospectco_co AS ( 
    SELECT *
    FROM bronze.cellphonealreadylinkedtoadifferentprospectco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonealreadylinkedtoexistingclientco_co AS ( 
    SELECT *
    FROM bronze.cellphonealreadylinkedtoexistingclientco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonelistedinblacklistco_co AS ( 
    SELECT *
    FROM bronze.cellphonelistedinblacklistco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginacceptancecodedidnotmatchco_co AS ( 
    SELECT *
    FROM bronze.checkoutloginacceptancecodedidnotmatchco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginacceptancenotificationfailedco_co AS ( 
    SELECT *
    FROM bronze.checkoutloginacceptancenotificationfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginacceptancewasexpiredco_co AS ( 
    SELECT *
    FROM bronze.checkoutloginacceptancewasexpiredco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginacceptedco_co AS ( 
    SELECT *
    FROM bronze.checkoutloginacceptedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginsentco_co AS ( 
    SELECT *
    FROM bronze.checkoutloginsentco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkouttransactioncompletedco_co AS ( 
    SELECT *
    FROM bronze.checkouttransactioncompletedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkouttransactionstartedco_co AS ( 
    SELECT *
    FROM bronze.checkouttransactionstartedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientaddicupowasbalancezeroco_co AS ( 
    SELECT *
    FROM bronze.clientaddicupowasbalancezeroco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientaddicupowasbalancezeropagoco_co AS ( 
    SELECT *
    FROM bronze.clientaddicupowasbalancezeropagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailedco_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailedpagoco_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientfailedtoobtainbureauinformationpagoco_co AS ( 
    SELECT *
    FROM bronze.clientfailedtoobtainbureauinformationpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientfailedtoobtainscoringco_co AS ( 
    SELECT *
    FROM bronze.clientfailedtoobtainscoringco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientfailedtoobtainscoringpagoco_co AS ( 
    SELECT *
    FROM bronze.clientfailedtoobtainscoringpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientfirstlastnameconfirmedco_co AS ( 
    SELECT *
    FROM bronze.clientfirstlastnameconfirmedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientfraudcheckfailedco_co AS ( 
    SELECT *
    FROM bronze.clientfraudcheckfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientfraudcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.clientfraudcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clienthasfootprintfromblacklistedentityco_co AS ( 
    SELECT *
    FROM bronze.clienthasfootprintfromblacklistedentityco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientinformationobtainedsantanderco_co AS ( 
    SELECT *
    FROM bronze.clientinformationobtainedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientnationalidentificationexpeditiondateconfirmedco_co AS ( 
    SELECT *
    FROM bronze.clientnationalidentificationexpeditiondateconfirmedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentbehaviorvalidationapprovedco_co AS ( 
    SELECT *
    FROM bronze.clientpaymentbehaviorvalidationapprovedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentbehaviorvalidationapprovedpagoco_co AS ( 
    SELECT *
    FROM bronze.clientpaymentbehaviorvalidationapprovedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentbehaviorvalidationrejectedco_co AS ( 
    SELECT *
    FROM bronze.clientpaymentbehaviorvalidationrejectedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpaymentbehaviorvalidationrejectedpagoco_co AS ( 
    SELECT *
    FROM bronze.clientpaymentbehaviorvalidationrejectedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpreapprovaljourneyisdisabledpagoco_co AS ( 
    SELECT *
    FROM bronze.clientpreapprovaljourneyisdisabledpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientwaspreapprovedbeforepagoco_co AS ( 
    SELECT *
    FROM bronze.clientwaspreapprovedbeforepagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpsychometricpassedco_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpsychometricpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,createcasefailedsantanderco_co AS ( 
    SELECT *
    FROM bronze.createcasefailedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckapprovedsantanderco_co AS ( 
    SELECT *
    FROM bronze.creditcheckapprovedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedco_co AS ( 
    SELECT *
    FROM bronze.creditcheckfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedpagoco_co AS ( 
    SELECT *
    FROM bronze.creditcheckfailedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedsantanderco_co AS ( 
    SELECT *
    FROM bronze.creditcheckfailedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpsychometricfailedco_co AS ( 
    SELECT *
    FROM bronze.creditcheckpsychometricfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpsychometricpassedco_co AS ( 
    SELECT *
    FROM bronze.creditcheckpsychometricpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckrejectedsantanderco_co AS ( 
    SELECT *
    FROM bronze.creditcheckrejectedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,documentlistedinblacklistco_co AS ( 
    SELECT *
    FROM bronze.documentlistedinblacklistco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,documentsobtainedsantanderco_co AS ( 
    SELECT *
    FROM bronze.documentsobtainedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,emailalreadylinkedtoexistingclientco_co AS ( 
    SELECT *
    FROM bronze.emailalreadylinkedtoexistingclientco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,emaillistedinblacklistco_co AS ( 
    SELECT *
    FROM bronze.emaillistedinblacklistco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,expeditedcheckoutloanproposalselectedco_co AS ( 
    SELECT *
    FROM bronze.expeditedcheckoutloanproposalselectedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtoobtainbureauinformationpagoco_co AS ( 
    SELECT *
    FROM bronze.failedtoobtainbureauinformationpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtoobtaincommercialinformationco_co AS ( 
    SELECT *
    FROM bronze.failedtoobtaincommercialinformationco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtoobtainscoringco_co AS ( 
    SELECT *
    FROM bronze.failedtoobtainscoringco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtoobtainscoringpagoco_co AS ( 
    SELECT *
    FROM bronze.failedtoobtainscoringpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtosendsignedfilessantanderco_co AS ( 
    SELECT *
    FROM bronze.failedtosendsignedfilessantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtosigndocumentssantanderco_co AS ( 
    SELECT *
    FROM bronze.failedtosigndocumentssantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtovalidateemailco_co AS ( 
    SELECT *
    FROM bronze.failedtovalidateemailco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,firstpaymentdatechangedco_co AS ( 
    SELECT *
    FROM bronze.firstpaymentdatechangedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,fraudcheckfailedco_co AS ( 
    SELECT *
    FROM bronze.fraudcheckfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,fraudcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.fraudcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,getclientinformationfailedsantanderco_co AS ( 
    SELECT *
    FROM bronze.getclientinformationfailedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,getdocumentsfailedsantanderco_co AS ( 
    SELECT *
    FROM bronze.getdocumentsfailedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosagentassigned_co AS ( 
    SELECT *
    FROM bronze.identityphotosagentassigned_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosapproved_co AS ( 
    SELECT *
    FROM bronze.identityphotosapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotoscollected_co AS ( 
    SELECT *
    FROM bronze.identityphotoscollected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosdiscarded_co AS ( 
    SELECT *
    FROM bronze.identityphotosdiscarded_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosdiscardedbyrisk_co AS ( 
    SELECT *
    FROM bronze.identityphotosdiscardedbyrisk_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosevaluationstarted_co AS ( 
    SELECT *
    FROM bronze.identityphotosevaluationstarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotoskeptbyagent_co AS ( 
    SELECT *
    FROM bronze.identityphotoskeptbyagent_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosrejected_co AS ( 
    SELECT *
    FROM bronze.identityphotosrejected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosstarted_co AS ( 
    SELECT *
    FROM bronze.identityphotosstarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyapproved_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscarded_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscarded_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartydiscardedbyrisk_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartydiscardedbyrisk_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartymanualverificationrequired_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartymanualverificationrequired_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrejected_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrejected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartyrequested_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartyrequested_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosthirdpartystarted_co AS ( 
    SELECT *
    FROM bronze.identityphotosthirdpartystarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywaagentassigned_co AS ( 
    SELECT *
    FROM bronze.identitywaagentassigned_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywaapproved_co AS ( 
    SELECT *
    FROM bronze.identitywaapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywadiscarded_co AS ( 
    SELECT *
    FROM bronze.identitywadiscarded_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywadiscardedbyrisk_co AS ( 
    SELECT *
    FROM bronze.identitywadiscardedbyrisk_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywainitialmessageresponsereceived_co AS ( 
    SELECT *
    FROM bronze.identitywainitialmessageresponsereceived_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywainputinformationcompleted_co AS ( 
    SELECT *
    FROM bronze.identitywainputinformationcompleted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywakeptbyagent_co AS ( 
    SELECT *
    FROM bronze.identitywakeptbyagent_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywarejected_co AS ( 
    SELECT *
    FROM bronze.identitywarejected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywastarted_co AS ( 
    SELECT *
    FROM bronze.identitywastarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,invalidemailco_co AS ( 
    SELECT *
    FROM bronze.invalidemailco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,leaddisabledtooriginateco_co AS ( 
    SELECT *
    FROM bronze.leaddisabledtooriginateco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,leaddisabledtooriginatepagoco_co AS ( 
    SELECT *
    FROM bronze.leaddisabledtooriginatepagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,listedinofacco_co AS ( 
    SELECT *
    FROM bronze.listedinofacco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancecodedidnotmatchco_co AS ( 
    SELECT *
    FROM bronze.loanacceptancecodedidnotmatchco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancecodedidnotmatchsantanderco_co AS ( 
    SELECT *
    FROM bronze.loanacceptancecodedidnotmatchsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancenotificationfailedco_co AS ( 
    SELECT *
    FROM bronze.loanacceptancenotificationfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancenotificationfailedsantanderco_co AS ( 
    SELECT *
    FROM bronze.loanacceptancenotificationfailedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancesentco_co AS ( 
    SELECT *
    FROM bronze.loanacceptancesentco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancesentsantanderco_co AS ( 
    SELECT *
    FROM bronze.loanacceptancesentsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancewasexpiredco_co AS ( 
    SELECT *
    FROM bronze.loanacceptancewasexpiredco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptancewasexpiredsantanderco_co AS ( 
    SELECT *
    FROM bronze.loanacceptancewasexpiredsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedbygatewaysantanderco_co AS ( 
    SELECT *
    FROM bronze.loanacceptedbygatewaysantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedco_co AS ( 
    SELECT *
    FROM bronze.loanacceptedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedsantanderco_co AS ( 
    SELECT *
    FROM bronze.loanacceptedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalselectedco_co AS ( 
    SELECT *
    FROM bronze.loanproposalselectedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalselectedsantanderco_co AS ( 
    SELECT *
    FROM bronze.loanproposalselectedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,originationunexpectederroroccurred_co AS ( 
    SELECT *
    FROM bronze.originationunexpectederroroccurred_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalapplicationcompletedco_co AS ( 
    SELECT *
    FROM bronze.preapprovalapplicationcompletedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalclienthasfootprintfromblacklistedentityco_co AS ( 
    SELECT *
    FROM bronze.preapprovalclienthasfootprintfromblacklistedentityco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalconditionalcreditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.preapprovalconditionalcreditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalconditionalcreditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.preapprovalconditionalcreditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckfailedco_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckfailedpagoco_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckfailedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalfailedtoobtainbureauinformationpagoco_co AS ( 
    SELECT *
    FROM bronze.preapprovalfailedtoobtainbureauinformationpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalfailedtoobtaincommercialinformationco_co AS ( 
    SELECT *
    FROM bronze.preapprovalfailedtoobtaincommercialinformationco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalfailedtoobtainscoringco_co AS ( 
    SELECT *
    FROM bronze.preapprovalfailedtoobtainscoringco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalfailedtoobtainscoringpagoco_co AS ( 
    SELECT *
    FROM bronze.preapprovalfailedtoobtainscoringpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalloanproposalswithinvalidusuryratesco_co AS ( 
    SELECT *
    FROM bronze.preapprovalloanproposalswithinvalidusuryratesco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalloanproposalswithinvalidusuryratespagoco_co AS ( 
    SELECT *
    FROM bronze.preapprovalloanproposalswithinvalidusuryratespagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preconditionswerevalidco_co AS ( 
    SELECT *
    FROM bronze.preconditionswerevalidco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preconditionswerevalidpagoco_co AS ( 
    SELECT *
    FROM bronze.preconditionswerevalidpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyacceptedco_co AS ( 
    SELECT *
    FROM bronze.privacypolicyacceptedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyacceptedsantanderco_co AS ( 
    SELECT *
    FROM bronze.privacypolicyacceptedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectalreadylinkedtodifferentcellphoneco_co AS ( 
    SELECT *
    FROM bronze.prospectalreadylinkedtodifferentcellphoneco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectdocumentnotfoundinbureauco_co AS ( 
    SELECT *
    FROM bronze.prospectdocumentnotfoundinbureauco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectinfonotcompleteinbureauco_co AS ( 
    SELECT *
    FROM bronze.prospectinfonotcompleteinbureauco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationapproved_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationfailed_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationfailed_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationisrequiredco_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationisrequiredco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationisrequiredpagoco_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationisrequiredpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationrated_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationrated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationrejected_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationrejected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationstarted_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationstarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwasgreaterthanmaximumconfiguredco_co AS ( 
    SELECT *
    FROM bronze.requestedamountwasgreaterthanmaximumconfiguredco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwasgreaterthanmaximumconfiguredpagoco_co AS ( 
    SELECT *
    FROM bronze.requestedamountwasgreaterthanmaximumconfiguredpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwaslessthanminimumconfiguredco_co AS ( 
    SELECT *
    FROM bronze.requestedamountwaslessthanminimumconfiguredco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwaslessthanminimumconfiguredpagoco_co AS ( 
    SELECT *
    FROM bronze.requestedamountwaslessthanminimumconfiguredpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,sendloanacceptancemaxattemptsreachedco_co AS ( 
    SELECT *
    FROM bronze.sendloanacceptancemaxattemptsreachedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,sendloanacceptancemaxattemptsreachedsantanderco_co AS ( 
    SELECT *
    FROM bronze.sendloanacceptancemaxattemptsreachedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,signeddocumentssantanderco_co AS ( 
    SELECT *
    FROM bronze.signeddocumentssantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,workinformationreceivedsantanderco_co AS ( 
    SELECT *
    FROM bronze.workinformationreceivedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpendingapplicationfoundco_co AS ( 
    SELECT *
    FROM bronze.clientpendingapplicationfoundco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpendingapplicationfoundpagoco_co AS ( 
    SELECT *
    FROM bronze.clientpendingapplicationfoundpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedwithoutloan_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassedwithoutloan_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,returningclientjourneyisdisableco_co AS ( 
    SELECT *
    FROM bronze.returningclientjourneyisdisableco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidatedco_co AS ( 
    SELECT *
    FROM bronze.cellphonevalidatedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidationcodedidnotmatchco_co AS ( 
    SELECT *
    FROM bronze.cellphonevalidationcodedidnotmatchco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidationcodewasexpiredco_co AS ( 
    SELECT *
    FROM bronze.cellphonevalidationcodewasexpiredco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidationnotificationsentco_co AS ( 
    SELECT *
    FROM bronze.cellphonevalidationnotificationsentco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,personalinformationupdatedco_co AS ( 
    SELECT *
    FROM bronze.personalinformationupdatedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationclosedbynewone_co AS ( 
    SELECT *
    FROM bronze.applicationclosedbynewone_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonevalidationmaxattemptsreachedco_co AS ( 
    SELECT *
    FROM bronze.cellphonevalidationmaxattemptsreachedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientloanproposalswerewithinvalidusuryratepagoco_co AS ( 
    SELECT *
    FROM bronze.clientloanproposalswerewithinvalidusuryratepagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,sendcellphonevalidationnotificationmaxattemptsreachedco_co AS ( 
    SELECT *
    FROM bronze.sendcellphonevalidationnotificationmaxattemptsreachedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalswithinvalidusuryratesco_co AS ( 
    SELECT *
    FROM bronze.loanproposalswithinvalidusuryratesco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientinsufficientaddicupopagoco_co AS ( 
    SELECT *
    FROM bronze.clientinsufficientaddicupopagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM acceptloanmaxattemptsreachedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM acceptloanmaxattemptsreachedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM additionalinformationreceivedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM allyisdisabledtooriginateco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM allyisdisabledtooriginatepagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationadjustedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationcreated_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationdeclined_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationdeviceinformationupdated_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationdeviceupdated_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationexpired_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationrestarted_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM backgroundcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM basicidentityvalidatedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM basicidverificationfailedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM bioinfodidnotmatchco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM casecreatedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonealreadylinkedtoadifferentprospectco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonealreadylinkedtoexistingclientco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonelistedinblacklistco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkoutloginacceptancecodedidnotmatchco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkoutloginacceptancenotificationfailedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkoutloginacceptancewasexpiredco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkoutloginacceptedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkoutloginsentco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkouttransactioncompletedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM checkouttransactionstartedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientaddicupowasbalancezeroco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientaddicupowasbalancezeropagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientcreditcheckfailedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientcreditcheckfailedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientcreditcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientcreditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientfailedtoobtainbureauinformationpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientfailedtoobtainscoringco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientfailedtoobtainscoringpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientfirstlastnameconfirmedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientfraudcheckfailedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientfraudcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clienthasfootprintfromblacklistedentityco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientinformationobtainedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientnationalidentificationexpeditiondateconfirmedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpaymentbehaviorvalidationapprovedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpaymentbehaviorvalidationapprovedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpaymentbehaviorvalidationrejectedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpaymentbehaviorvalidationrejectedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpreapprovaljourneyisdisabledpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientwaspreapprovedbeforepagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM conditionalcreditcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM conditionalcreditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM conditionalcreditcheckpsychometricpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM createcasefailedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckapprovedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckfailedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckfailedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckfailedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckpsychometricfailedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckpsychometricpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckrejectedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM documentlistedinblacklistco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM documentsobtainedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM emailalreadylinkedtoexistingclientco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM emaillistedinblacklistco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM expeditedcheckoutloanproposalselectedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM failedtoobtainbureauinformationpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM failedtoobtaincommercialinformationco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM failedtoobtainscoringco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM failedtoobtainscoringpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM failedtosendsignedfilessantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM failedtosigndocumentssantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM failedtovalidateemailco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM firstpaymentdatechangedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM fraudcheckfailedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM fraudcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM getclientinformationfailedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM getdocumentsfailedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosagentassigned_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosapproved_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotoscollected_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosdiscarded_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosdiscardedbyrisk_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosevaluationstarted_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotoskeptbyagent_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosrejected_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosstarted_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosthirdpartyapproved_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosthirdpartydiscarded_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosthirdpartydiscardedbyrisk_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosthirdpartymanualverificationrequired_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosthirdpartyrejected_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosthirdpartyrequested_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identityphotosthirdpartystarted_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywaagentassigned_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywaapproved_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywadiscarded_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywadiscardedbyrisk_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywainitialmessageresponsereceived_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywainputinformationcompleted_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywakeptbyagent_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywarejected_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM identitywastarted_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM invalidemailco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM leaddisabledtooriginateco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM leaddisabledtooriginatepagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM listedinofacco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancecodedidnotmatchco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancecodedidnotmatchsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancenotificationfailedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancenotificationfailedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancesentco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancesentsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancewasexpiredco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptancewasexpiredsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptedbygatewaysantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanacceptedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanproposalselectedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanproposalselectedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM originationunexpectederroroccurred_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalapplicationcompletedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalclienthasfootprintfromblacklistedentityco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalconditionalcreditcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalconditionalcreditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalcreditcheckfailedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalcreditcheckfailedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalcreditcheckpassedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalcreditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalfailedtoobtainbureauinformationpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalfailedtoobtaincommercialinformationco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalfailedtoobtainscoringco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalfailedtoobtainscoringpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalloanproposalswithinvalidusuryratesco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preapprovalloanproposalswithinvalidusuryratespagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preconditionswerevalidco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM preconditionswerevalidpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM privacypolicyacceptedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM privacypolicyacceptedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM prospectalreadylinkedtodifferentcellphoneco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM prospectdocumentnotfoundinbureauco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM prospectinfonotcompleteinbureauco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationapproved_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationfailed_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationisrequiredco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationisrequiredpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationrated_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationrejected_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM psychometricevaluationstarted_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM requestedamountwasgreaterthanmaximumconfiguredco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM requestedamountwasgreaterthanmaximumconfiguredpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM requestedamountwaslessthanminimumconfiguredco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM requestedamountwaslessthanminimumconfiguredpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM sendloanacceptancemaxattemptsreachedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM sendloanacceptancemaxattemptsreachedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM signeddocumentssantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM workinformationreceivedsantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpendingapplicationfoundco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientpendingapplicationfoundpagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM creditcheckpassedwithoutloan_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM returningclientjourneyisdisableco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidatedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidationcodedidnotmatchco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidationcodewasexpiredco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidationnotificationsentco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM personalinformationupdatedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM applicationclosedbynewone_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM cellphonevalidationmaxattemptsreachedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientloanproposalswerewithinvalidusuryratepagoco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM sendcellphonevalidationnotificationmaxattemptsreachedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM loanproposalswithinvalidusuryratesco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,channel,client_id,client_type,event_type,journey_name,journey_stage_name,ocurred_on,product,
    event_name,
    event_id
    FROM clientinsufficientaddicupopagoco_co
    
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
this: silver.f_origination_events_co_logs
country: co
silver_table_name: f_origination_events_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'application_id', 'channel', 'client_id', 'client_type', 'event_name', 'event_type', 'journey_name', 'journey_stage_name', 'ocurred_on', 'product']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'acceptloanmaxattemptsreachedco': {'stage': 'loan_acceptance_co', 'reference_order_id': 1, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'acceptloanmaxattemptsreachedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 2, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'additionalinformationreceivedsantanderco': {'stage': 'additional_information_santander_co', 'reference_order_id': 3, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'allyisdisabledtooriginateco': {'stage': 'preconditions_co', 'reference_order_id': 6, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'allyisdisabledtooriginatepagoco': {'stage': 'preconditions_pago_co', 'reference_order_id': 7, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationadjustedco': {'stage': 'GLOBAL', 'reference_order_id': 8, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationcreated': {'stage': 'GLOBAL', 'reference_order_id': 10, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationdeclined': {'stage': 'GLOBAL', 'reference_order_id': 11, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationdeviceinformationupdated': {'stage': 'device_information', 'reference_order_id': 12, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationdeviceupdated': {'stage': 'GLOBAL', 'reference_order_id': 13, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationexpired': {'stage': 'GLOBAL', 'reference_order_id': 14, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationrestarted': {'stage': 'GLOBAL', 'reference_order_id': 15, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'backgroundcheckpassedco': {'stage': 'background_check_co', 'reference_order_id': 18, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'basicidentityvalidatedco': {'stage': 'basic_identity_co', 'reference_order_id': 21, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'basicidverificationfailedco': {'stage': 'basic_identity_co', 'reference_order_id': 23, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'bioinfodidnotmatchco': {'stage': 'basic_identity_co', 'reference_order_id': 25, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'casecreatedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 29, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonealreadylinkedtoadifferentprospectco': {'stage': 'background_check_co', 'reference_order_id': 31, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonealreadylinkedtoexistingclientco': {'stage': 'background_check_co', 'reference_order_id': 33, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonelistedinblacklistco': {'stage': 'background_check_co', 'reference_order_id': 35, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkoutloginacceptancecodedidnotmatchco': {'stage': 'expedited_checkout_login_co', 'reference_order_id': 42, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkoutloginacceptancenotificationfailedco': {'stage': 'expedited_checkout_login_co', 'reference_order_id': 43, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkoutloginacceptancewasexpiredco': {'stage': 'expedited_checkout_login_co', 'reference_order_id': 45, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkoutloginacceptedco': {'stage': 'expedited_checkout_login_co', 'reference_order_id': 47, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkoutloginsentco': {'stage': 'expedited_checkout_login_co', 'reference_order_id': 49, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkouttransactioncompletedco': {'stage': 'expedited_checkout_transaction_co', 'reference_order_id': 51, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'checkouttransactionstartedco': {'stage': 'expedited_checkout_transaction_co', 'reference_order_id': 53, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientaddicupowasbalancezeroco': {'stage': 'underwriting_rc_co', 'reference_order_id': 55, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientaddicupowasbalancezeropagoco': {'stage': 'underwriting_rc_pago_co', 'reference_order_id': 56, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientcreditcheckfailedco': {'stage': 'underwriting_rc_co', 'reference_order_id': 59, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientcreditcheckfailedpagoco': {'stage': 'underwriting_rc_pago_co', 'reference_order_id': 60, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientcreditcheckpassedco': {'stage': 'underwriting_rc_co', 'reference_order_id': 62, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientcreditcheckpassedpagoco': {'stage': 'underwriting_rc_pago_co', 'reference_order_id': 63, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientfailedtoobtainbureauinformationpagoco': {'stage': 'underwriting_rc_pago_co', 'reference_order_id': 64, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientfailedtoobtainscoringco': {'stage': 'underwriting_rc_co', 'reference_order_id': 66, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientfailedtoobtainscoringpagoco': {'stage': 'underwriting_rc_pago_co', 'reference_order_id': 67, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientfirstlastnameconfirmedco': {'stage': 'privacy_policy_co', 'reference_order_id': 68, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientfraudcheckfailedco': {'stage': 'fraud_check_rc_co', 'reference_order_id': 69, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientfraudcheckpassedco': {'stage': 'fraud_check_rc_co', 'reference_order_id': 71, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clienthasfootprintfromblacklistedentityco': {'stage': 'underwriting_co', 'reference_order_id': 72, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientinformationobtainedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 73, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientnationalidentificationexpeditiondateconfirmedco': {'stage': 'privacy_policy_co', 'reference_order_id': 75, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpaymentbehaviorvalidationapprovedco': {'stage': 'underwriting_rc_co', 'reference_order_id': 77, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpaymentbehaviorvalidationapprovedpagoco': {'stage': 'underwriting_rc_pago_co', 'reference_order_id': 78, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpaymentbehaviorvalidationrejectedco': {'stage': 'underwriting_rc_co', 'reference_order_id': 80, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpaymentbehaviorvalidationrejectedpagoco': {'stage': 'underwriting_rc_pago_co', 'reference_order_id': 81, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpreapprovaljourneyisdisabledpagoco': {'stage': 'preconditions_pago_co', 'reference_order_id': 83, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientwaspreapprovedbeforepagoco': {'stage': 'preconditions_pago_co', 'reference_order_id': 85, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'conditionalcreditcheckpassedco': {'stage': 'underwriting_co', 'reference_order_id': 87, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'conditionalcreditcheckpassedpagoco': {'stage': 'underwriting_pago_co', 'reference_order_id': 88, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'conditionalcreditcheckpsychometricpassedco': {'stage': 'underwriting_psychometric_co', 'reference_order_id': 90, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'createcasefailedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 91, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckapprovedsantanderco': {'stage': 'risk_evaluation_santander_co', 'reference_order_id': 92, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckfailedco': {'stage': 'underwriting_co', 'reference_order_id': 94, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckfailedpagoco': {'stage': 'underwriting_pago_co', 'reference_order_id': 95, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckfailedsantanderco': {'stage': 'risk_evaluation_santander_co', 'reference_order_id': 96, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckpassedco': {'stage': 'underwriting_co', 'reference_order_id': 98, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckpassedpagoco': {'stage': 'underwriting_pago_co', 'reference_order_id': 99, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckpsychometricfailedco': {'stage': 'underwriting_psychometric_co', 'reference_order_id': 101, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckpsychometricpassedco': {'stage': 'underwriting_psychometric_co', 'reference_order_id': 102, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckrejectedsantanderco': {'stage': 'risk_evaluation_santander_co', 'reference_order_id': 103, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'documentlistedinblacklistco': {'stage': 'background_check_co', 'reference_order_id': 105, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'documentsobtainedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 106, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'emailalreadylinkedtoexistingclientco': {'stage': 'background_check_co', 'reference_order_id': 108, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'emaillistedinblacklistco': {'stage': 'background_check_co', 'reference_order_id': 110, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'expeditedcheckoutloanproposalselectedco': {'stage': 'expedited_loan_proposals_co', 'reference_order_id': 111, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'failedtoobtainbureauinformationpagoco': {'stage': 'underwriting_pago_co', 'reference_order_id': 119, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'failedtoobtaincommercialinformationco': {'stage': 'underwriting_co', 'reference_order_id': 121, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'failedtoobtainscoringco': {'stage': 'underwriting_co', 'reference_order_id': 123, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'failedtoobtainscoringpagoco': {'stage': 'underwriting_pago_co', 'reference_order_id': 124, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'failedtosendsignedfilessantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 125, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'failedtosigndocumentssantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 126, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'failedtovalidateemailco': {'stage': 'background_check_co', 'reference_order_id': 127, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'firstpaymentdatechangedco': {'stage': 'loan_proposals_co', 'reference_order_id': 128, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'fraudcheckfailedco': {'stage': 'fraud_check_co', 'reference_order_id': 130, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'fraudcheckpassedco': {'stage': 'fraud_check_co', 'reference_order_id': 132, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'getclientinformationfailedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 133, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'getdocumentsfailedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 134, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosagentassigned': {'stage': 'identity_photos', 'reference_order_id': 135, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosapproved': {'stage': 'identity_photos', 'reference_order_id': 136, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotoscollected': {'stage': 'identity_photos', 'reference_order_id': 137, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosdiscarded': {'stage': 'identity_photos', 'reference_order_id': 138, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosdiscardedbyrisk': {'stage': 'identity_photos', 'reference_order_id': 139, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosevaluationstarted': {'stage': 'identity_photos', 'reference_order_id': 140, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotoskeptbyagent': {'stage': 'identity_photos', 'reference_order_id': 141, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosrejected': {'stage': 'identity_photos', 'reference_order_id': 142, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosstarted': {'stage': 'identity_photos', 'reference_order_id': 143, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosthirdpartyapproved': {'stage': 'identity_photos', 'reference_order_id': 144, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscarded': {'stage': 'identity_photos', 'reference_order_id': 145, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosthirdpartydiscardedbyrisk': {'stage': 'identity_photos', 'reference_order_id': 146, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosthirdpartymanualverificationrequired': {'stage': 'identity_photos', 'reference_order_id': 147, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosthirdpartyrejected': {'stage': 'identity_photos', 'reference_order_id': 148, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosthirdpartyrequested': {'stage': 'identity_photos', 'reference_order_id': 149, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identityphotosthirdpartystarted': {'stage': 'identity_photos', 'reference_order_id': 150, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywaagentassigned': {'stage': 'identity_wa', 'reference_order_id': 151, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywaapproved': {'stage': 'identity_wa', 'reference_order_id': 152, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywadiscarded': {'stage': 'identity_wa', 'reference_order_id': 153, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywadiscardedbyrisk': {'stage': 'identity_wa', 'reference_order_id': 154, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywainitialmessageresponsereceived': {'stage': 'identity_wa', 'reference_order_id': 155, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywainputinformationcompleted': {'stage': 'identity_wa', 'reference_order_id': 156, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywakeptbyagent': {'stage': 'identity_wa', 'reference_order_id': 157, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywarejected': {'stage': 'identity_wa', 'reference_order_id': 158, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'identitywastarted': {'stage': 'identity_wa', 'reference_order_id': 159, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'invalidemailco': {'stage': 'background_check_co', 'reference_order_id': 169, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'leaddisabledtooriginateco': {'stage': 'preconditions_co', 'reference_order_id': 170, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'leaddisabledtooriginatepagoco': {'stage': 'preconditions_pago_co', 'reference_order_id': 171, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'listedinofacco': {'stage': 'background_check_co', 'reference_order_id': 172, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancecodedidnotmatchco': {'stage': 'loan_acceptance_co', 'reference_order_id': 174, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancecodedidnotmatchsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 175, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancenotificationfailedco': {'stage': 'loan_acceptance_co', 'reference_order_id': 177, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancenotificationfailedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 178, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancesentco': {'stage': 'loan_acceptance_co', 'reference_order_id': 180, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancesentsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 181, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancewasexpiredco': {'stage': 'loan_acceptance_co', 'reference_order_id': 183, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptancewasexpiredsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 184, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptedbygatewaysantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 187, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptedco': {'stage': 'loan_acceptance_co', 'reference_order_id': 188, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanacceptedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 189, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanproposalselectedco': {'stage': 'loan_proposals_co', 'reference_order_id': 193, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanproposalselectedsantanderco': {'stage': 'loan_proposals_santander_co', 'reference_order_id': 194, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'originationunexpectederroroccurred': {'stage': 'GLOBAL', 'reference_order_id': 197, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalapplicationcompletedco': {'stage': 'preapproval_summary_co', 'reference_order_id': 210, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalclienthasfootprintfromblacklistedentityco': {'stage': 'underwriting_preapproval_co', 'reference_order_id': 211, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalconditionalcreditcheckpassedco': {'stage': 'underwriting_preapproval_co', 'reference_order_id': 213, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalconditionalcreditcheckpassedpagoco': {'stage': 'underwriting_preapproval_pago_co', 'reference_order_id': 214, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalcreditcheckfailedco': {'stage': 'underwriting_preapproval_co', 'reference_order_id': 216, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalcreditcheckfailedpagoco': {'stage': 'underwriting_preapproval_pago_co', 'reference_order_id': 217, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalcreditcheckpassedco': {'stage': 'underwriting_preapproval_co', 'reference_order_id': 219, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalcreditcheckpassedpagoco': {'stage': 'underwriting_preapproval_pago_co', 'reference_order_id': 220, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalfailedtoobtainbureauinformationpagoco': {'stage': 'underwriting_preapproval_pago_co', 'reference_order_id': 221, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalfailedtoobtaincommercialinformationco': {'stage': 'underwriting_preapproval_co', 'reference_order_id': 223, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalfailedtoobtainscoringco': {'stage': 'underwriting_preapproval_co', 'reference_order_id': 225, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalfailedtoobtainscoringpagoco': {'stage': 'underwriting_preapproval_pago_co', 'reference_order_id': 226, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalloanproposalswithinvalidusuryratesco': {'stage': 'underwriting_preapproval_co', 'reference_order_id': 227, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preapprovalloanproposalswithinvalidusuryratespagoco': {'stage': 'underwriting_preapproval_pago_co', 'reference_order_id': 228, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preconditionswerevalidco': {'stage': 'preconditions_co', 'reference_order_id': 230, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'preconditionswerevalidpagoco': {'stage': 'preconditions_pago_co', 'reference_order_id': 231, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'privacypolicyacceptedco': {'stage': 'privacy_policy_co', 'reference_order_id': 233, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'privacypolicyacceptedsantanderco': {'stage': 'privacy_policy_santander_co', 'reference_order_id': 234, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'prospectalreadylinkedtodifferentcellphoneco': {'stage': 'background_check_co', 'reference_order_id': 236, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'prospectdocumentnotfoundinbureauco': {'stage': 'basic_identity_co', 'reference_order_id': 238, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'prospectinfonotcompleteinbureauco': {'stage': 'basic_identity_co', 'reference_order_id': 240, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationapproved': {'stage': 'psychometric_assessment', 'reference_order_id': 241, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationfailed': {'stage': 'psychometric_assessment', 'reference_order_id': 242, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationisrequiredco': {'stage': 'underwriting_co', 'reference_order_id': 244, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationisrequiredpagoco': {'stage': 'underwriting_pago_co', 'reference_order_id': 245, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationrated': {'stage': 'psychometric_assessment', 'reference_order_id': 246, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationrejected': {'stage': 'psychometric_assessment', 'reference_order_id': 247, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'psychometricevaluationstarted': {'stage': 'psychometric_assessment', 'reference_order_id': 248, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'requestedamountwasgreaterthanmaximumconfiguredco': {'stage': 'preconditions_co', 'reference_order_id': 250, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'requestedamountwasgreaterthanmaximumconfiguredpagoco': {'stage': 'preconditions_pago_co', 'reference_order_id': 251, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'requestedamountwaslessthanminimumconfiguredco': {'stage': 'preconditions_co', 'reference_order_id': 253, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'requestedamountwaslessthanminimumconfiguredpagoco': {'stage': 'preconditions_pago_co', 'reference_order_id': 254, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'sendloanacceptancemaxattemptsreachedco': {'stage': 'loan_acceptance_co', 'reference_order_id': 257, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'sendloanacceptancemaxattemptsreachedsantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 258, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'signeddocumentssantanderco': {'stage': 'loan_acceptance_santander_co', 'reference_order_id': 259, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'workinformationreceivedsantanderco': {'stage': 'work_information_santander_co', 'reference_order_id': 261, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpendingapplicationfoundco': {'stage': 'preconditions_co', 'reference_order_id': 262, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientpendingapplicationfoundpagoco': {'stage': 'preconditions_pago_co', 'reference_order_id': 263, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'creditcheckpassedwithoutloan': {'stage': 'underwriting', 'reference_order_id': 264, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'returningclientjourneyisdisableco': {'stage': 'preconditions_co', 'reference_order_id': 265, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidatedco': {'stage': 'cellphone_validation_co', 'reference_order_id': 266, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidationcodedidnotmatchco': {'stage': 'cellphone_validation_co', 'reference_order_id': 267, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidationcodewasexpiredco': {'stage': 'cellphone_validation_co', 'reference_order_id': 268, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidationnotificationsentco': {'stage': 'cellphone_validation_co', 'reference_order_id': 269, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'personalinformationupdatedco': {'stage': 'personal_information_co', 'reference_order_id': 270, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'applicationclosedbynewone': {'stage': 'GLOBAL', 'reference_order_id': 271, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'cellphonevalidationmaxattemptsreachedco': {'stage': 'cellphone_validation_co', 'reference_order_id': 272, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientloanproposalswerewithinvalidusuryratepagoco': {'stage': 'underwriting_rc_pago_co', 'reference_order_id': 273, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'sendcellphonevalidationnotificationmaxattemptsreachedco': {'stage': 'cellphone_validation_co', 'reference_order_id': 274, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'loanproposalswithinvalidusuryratesco': {'stage': 'underwriting_co', 'reference_order_id': 275, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}, 'clientinsufficientaddicupopagoco': {'stage': 'preconditions_pago_co', 'reference_order_id': 276, 'direct_attributes': ['application_id', 'client_id', 'journey_name', 'event_name', 'ocurred_on', 'journey_stage_name', 'event_type', 'ally_slug', 'product', 'client_type', 'channel'], 'custom_attributes': {}}}
events_keys: ['acceptloanmaxattemptsreachedco', 'acceptloanmaxattemptsreachedsantanderco', 'additionalinformationreceivedsantanderco', 'allyisdisabledtooriginateco', 'allyisdisabledtooriginatepagoco', 'applicationadjustedco', 'applicationcreated', 'applicationdeclined', 'applicationdeviceinformationupdated', 'applicationdeviceupdated', 'applicationexpired', 'applicationrestarted', 'backgroundcheckpassedco', 'basicidentityvalidatedco', 'basicidverificationfailedco', 'bioinfodidnotmatchco', 'casecreatedsantanderco', 'cellphonealreadylinkedtoadifferentprospectco', 'cellphonealreadylinkedtoexistingclientco', 'cellphonelistedinblacklistco', 'checkoutloginacceptancecodedidnotmatchco', 'checkoutloginacceptancenotificationfailedco', 'checkoutloginacceptancewasexpiredco', 'checkoutloginacceptedco', 'checkoutloginsentco', 'checkouttransactioncompletedco', 'checkouttransactionstartedco', 'clientaddicupowasbalancezeroco', 'clientaddicupowasbalancezeropagoco', 'clientcreditcheckfailedco', 'clientcreditcheckfailedpagoco', 'clientcreditcheckpassedco', 'clientcreditcheckpassedpagoco', 'clientfailedtoobtainbureauinformationpagoco', 'clientfailedtoobtainscoringco', 'clientfailedtoobtainscoringpagoco', 'clientfirstlastnameconfirmedco', 'clientfraudcheckfailedco', 'clientfraudcheckpassedco', 'clienthasfootprintfromblacklistedentityco', 'clientinformationobtainedsantanderco', 'clientnationalidentificationexpeditiondateconfirmedco', 'clientpaymentbehaviorvalidationapprovedco', 'clientpaymentbehaviorvalidationapprovedpagoco', 'clientpaymentbehaviorvalidationrejectedco', 'clientpaymentbehaviorvalidationrejectedpagoco', 'clientpreapprovaljourneyisdisabledpagoco', 'clientwaspreapprovedbeforepagoco', 'conditionalcreditcheckpassedco', 'conditionalcreditcheckpassedpagoco', 'conditionalcreditcheckpsychometricpassedco', 'createcasefailedsantanderco', 'creditcheckapprovedsantanderco', 'creditcheckfailedco', 'creditcheckfailedpagoco', 'creditcheckfailedsantanderco', 'creditcheckpassedco', 'creditcheckpassedpagoco', 'creditcheckpsychometricfailedco', 'creditcheckpsychometricpassedco', 'creditcheckrejectedsantanderco', 'documentlistedinblacklistco', 'documentsobtainedsantanderco', 'emailalreadylinkedtoexistingclientco', 'emaillistedinblacklistco', 'expeditedcheckoutloanproposalselectedco', 'failedtoobtainbureauinformationpagoco', 'failedtoobtaincommercialinformationco', 'failedtoobtainscoringco', 'failedtoobtainscoringpagoco', 'failedtosendsignedfilessantanderco', 'failedtosigndocumentssantanderco', 'failedtovalidateemailco', 'firstpaymentdatechangedco', 'fraudcheckfailedco', 'fraudcheckpassedco', 'getclientinformationfailedsantanderco', 'getdocumentsfailedsantanderco', 'identityphotosagentassigned', 'identityphotosapproved', 'identityphotoscollected', 'identityphotosdiscarded', 'identityphotosdiscardedbyrisk', 'identityphotosevaluationstarted', 'identityphotoskeptbyagent', 'identityphotosrejected', 'identityphotosstarted', 'identityphotosthirdpartyapproved', 'identityphotosthirdpartydiscarded', 'identityphotosthirdpartydiscardedbyrisk', 'identityphotosthirdpartymanualverificationrequired', 'identityphotosthirdpartyrejected', 'identityphotosthirdpartyrequested', 'identityphotosthirdpartystarted', 'identitywaagentassigned', 'identitywaapproved', 'identitywadiscarded', 'identitywadiscardedbyrisk', 'identitywainitialmessageresponsereceived', 'identitywainputinformationcompleted', 'identitywakeptbyagent', 'identitywarejected', 'identitywastarted', 'invalidemailco', 'leaddisabledtooriginateco', 'leaddisabledtooriginatepagoco', 'listedinofacco', 'loanacceptancecodedidnotmatchco', 'loanacceptancecodedidnotmatchsantanderco', 'loanacceptancenotificationfailedco', 'loanacceptancenotificationfailedsantanderco', 'loanacceptancesentco', 'loanacceptancesentsantanderco', 'loanacceptancewasexpiredco', 'loanacceptancewasexpiredsantanderco', 'loanacceptedbygatewaysantanderco', 'loanacceptedco', 'loanacceptedsantanderco', 'loanproposalselectedco', 'loanproposalselectedsantanderco', 'originationunexpectederroroccurred', 'preapprovalapplicationcompletedco', 'preapprovalclienthasfootprintfromblacklistedentityco', 'preapprovalconditionalcreditcheckpassedco', 'preapprovalconditionalcreditcheckpassedpagoco', 'preapprovalcreditcheckfailedco', 'preapprovalcreditcheckfailedpagoco', 'preapprovalcreditcheckpassedco', 'preapprovalcreditcheckpassedpagoco', 'preapprovalfailedtoobtainbureauinformationpagoco', 'preapprovalfailedtoobtaincommercialinformationco', 'preapprovalfailedtoobtainscoringco', 'preapprovalfailedtoobtainscoringpagoco', 'preapprovalloanproposalswithinvalidusuryratesco', 'preapprovalloanproposalswithinvalidusuryratespagoco', 'preconditionswerevalidco', 'preconditionswerevalidpagoco', 'privacypolicyacceptedco', 'privacypolicyacceptedsantanderco', 'prospectalreadylinkedtodifferentcellphoneco', 'prospectdocumentnotfoundinbureauco', 'prospectinfonotcompleteinbureauco', 'psychometricevaluationapproved', 'psychometricevaluationfailed', 'psychometricevaluationisrequiredco', 'psychometricevaluationisrequiredpagoco', 'psychometricevaluationrated', 'psychometricevaluationrejected', 'psychometricevaluationstarted', 'requestedamountwasgreaterthanmaximumconfiguredco', 'requestedamountwasgreaterthanmaximumconfiguredpagoco', 'requestedamountwaslessthanminimumconfiguredco', 'requestedamountwaslessthanminimumconfiguredpagoco', 'sendloanacceptancemaxattemptsreachedco', 'sendloanacceptancemaxattemptsreachedsantanderco', 'signeddocumentssantanderco', 'workinformationreceivedsantanderco', 'clientpendingapplicationfoundco', 'clientpendingapplicationfoundpagoco', 'creditcheckpassedwithoutloan', 'returningclientjourneyisdisableco', 'cellphonevalidatedco', 'cellphonevalidationcodedidnotmatchco', 'cellphonevalidationcodewasexpiredco', 'cellphonevalidationnotificationsentco', 'personalinformationupdatedco', 'applicationclosedbynewone', 'cellphonevalidationmaxattemptsreachedco', 'clientloanproposalswerewithinvalidusuryratepagoco', 'sendcellphonevalidationnotificationmaxattemptsreachedco', 'loanproposalswithinvalidusuryratesco', 'clientinsufficientaddicupopagoco']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
