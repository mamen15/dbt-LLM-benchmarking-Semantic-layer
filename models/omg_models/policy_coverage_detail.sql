select
    Effective_Date,
    Policy_Coverage_Detail_Identifier,
    Coverage_Identifier,
    Insurable_Object_Identifier,
    Policy_Identifier,
    Coverage_Part_Code,
    Coverage_Description,
    Expiration_Date,
    Coverage_Inclusion_Exclusion_Code
from dbt_cdw_benchmark__seed.policy_coverage_detail
