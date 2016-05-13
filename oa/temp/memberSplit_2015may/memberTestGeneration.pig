set default_parallel 5


Mem0 = LOAD '$MemCombined' USING PigStorage('\u0001') AS (
        membership_nbr: chararray,
        membership_create_date: chararray,
        membership_obsolete_date: chararray,
        membership_type_code: int,
        member_status_code: chararray,
        plus_membership_ind: chararray,
        pdcardholder_nbr: chararray,
        pdcardholder_type_code: chararray,
        pdcardholder_status_code: chararray,
        pdcardholder_issuing_club_nbr: chararray,
        pdcardholder_assigned_club_nbr: chararray,
        pdcardholder_preferred_club_nbr: chararray,
        cardholders:bag{(cardholder_nbr: chararray, cardholder_type_code: chararray, cardholder_status_code: chararray, issuing_club_nbr: chararray, assigned_club_nbr: chararray, preferred_club_nbr: chararray)}
);

MemberTest = FOREACH Mem0 GENERATE membership_nbr, pdcardholder_nbr, membership_create_date;

STORE MemberTest INTO '$MemberTest' Using PigStorage('\t');


