-- back compat for old kwarg name
  
  
  
      
          
          
      
  

  

  merge into `addi_prod`.`bronze`.`failedtosendsignedfilessantanderco_co` as DBT_INTERNAL_DEST
      using `failedtosendsignedfilessantanderco_co__dbt_tmp` as DBT_INTERNAL_SOURCE
      on 
              DBT_INTERNAL_SOURCE.event_id = DBT_INTERNAL_DEST.event_id
          

      when matched then update set
         * 

      when not matched then insert *
