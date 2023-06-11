USE [MAXIdev];
GO

CREATE  procedure [dbo].[st_ReportProfitVCountryV2]
(
		@IdCountryCurrency INT ,
        @StartDate DATETIME ,
        @EndDate DATETIME ,
        @IdUserSeller INT ,
        @IdUserRequester INT ,
        @State NVARCHAR(2) ,
		@Range1 MONEY  ,
		@Range2 MONEY ,
        @Type INT = NULL
)
as          
/********************************************************************
<Author>???</Author>
<app>Corporate</app>
<Description>Gets Profit Report</Description>

<ChangeLog>
<log Date="12/06/2017" Author="Forced Indexes in TransferClosed">   </log>
<log Date="24/01/2018" Author="jdarellano" Name="#1">Performance: se elimina index forzado de tabla "AgentBalance".</log>
<log Date="26/01/2018" Author="jmolina" Name="#2">Performance: Mejora en proceso de consultas".</log>
<log Date="01/11/2018" Author="jmolina" Name="#3">Se agrega filtro para payerconfig solo activos(IdGenericStatus = 1)".</log>
<log Date="01/03/2018" Author="jmolina" Name="#4">Se agrega IdCountryCurrency en las comisiones de pagadores y se agrega al JOIN de calculo de remesas".</log>
<log Date="07/03/2018" Author="jmolina" Name="#5">Cambia metodo de agrupado en calculos de remeasas, se elimina el over partition By y se asigna el GROUP BY)".</log>
<log Date="10/02/2022" Author="jcsierra" >Considera campos de TDD.</log>
<log Date="15/02/2022" Author="jcsierra" >Correccion al calcular columnas profit y margin</log>
<log Date="5/05/2023" Author="ccarrillo" >Agregando formato al SQL y atomicidad transaccional con el TRY/CATH </log>

</ChangeLog>

*********************************************************************/
--------------------------------
BEGIN TRY
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    --------------------------------
    DROP TABLE IF EXISTS #tempSC,
                         #Result,
                         #temp6,
                         #temp7,
                         #temp8,
                         #temp9,
                         #tDepositAgent,
                         #tOtherProd,
                         #temp10,
                         #temp6a,
                         #bankcommission,
                         #payercommission,
                         #SellerSubordinates,
                         #SellerTree,
                         #TempAgents,
                         #Temp,
                         #temp1a,
                         #temp1,
                         #temp2,
                         #temp2a,
                         #temp3,
                         #temp3a,
                         #temp5a,
                         #temp5,
                         #temp4,
                         #temp4a;



    SET @Type = COALESCE(@Type, 1);
    SELECT @StartDate = CAST(@StartDate AS DATE),
           @EndDate = CAST(@EndDate + 1 AS DATE);

    DECLARE @Date1 DATETIME = CAST(DATEADD(dd, - (DAY(@StartDate) - 1), @StartDate) AS DATE),
            @Date2 DATETIME = CAST(DATEADD(dd, - (DAY(@EndDate) - 1), @EndDate) AS DATE),
            @ReportCorpType BIT = CASE
                                      WHEN EXISTS
                                           (
                                               SELECT 1 FROM Users WHERE IdUser = @IdUserRequester AND IdUserType = 1
                                           )
                                           AND @IdUserSeller = 0 THEN
                                          1
                                      ELSE
                                          0
                                  END;


    ---------Special Commission---------------

    -- Query to get Special Commission for the report
    SELECT SC.IdAgent,
           SUM(SC.Commission) SpecialCommission
    INTO #tempSC
    FROM [dbo].[SpecialCommissionBalance] SC
        JOIN SpecialCommissionRule r
            ON SC.IdSpecialCommissionRule = r.IdSpecialCommissionRule
    WHERE SC.[DateOfApplication] >= @StartDate
          AND SC.[DateOfApplication] < @EndDate
    GROUP BY SC.IdAgent;

    -- Query to get Bank Commissions for the report
    SELECT DISTINCT
           DateOfBankCommission,
           FactorNew
    INTO #bankcommission
    FROM BankCommission
    WHERE Active = 1
          AND DateOfBankCommission >= @Date1
          AND DateOfBankCommission <= @Date2;

    -- Query to get Payer Commissions for the report
    SELECT DISTINCT
           IdGateway,
           IdPayer,
           IdPaymentType,
           CommissionNew,
           DateOfPayerConfigCommission,
           IdCountryCurrency
    INTO #payercommission
    FROM PayerConfigCommission c
        JOIN PayerConfig x
            ON c.IdPayerConfig = x.IdPayerConfig
               AND x.IdPayerConfig NOT IN ( 711, 807 )
    WHERE Active = 1
          AND DateOfPayerConfigCommission >= @Date1
          AND DateOfPayerConfigCommission <= @Date2
          AND x.IdGenericStatus = 1;

    -- Check if user has all sellers access
    DECLARE @IsAllSeller BIT =
            (
                SELECT TOP 1
                       1
                FROM [Users]
                WHERE @IdUserSeller = 0
                      AND [IdUser] = @IdUserRequester
                      AND [IdUserType] = 1
            );

    -- Create a temporary table to store Seller Subordinates
    CREATE TABLE #SellerSubordinates
    (
        IdSeller INT
    );


    -------Nuevo proceso de busqueda recursiva de Sellers---------------------
    WITH items
    AS (SELECT IdUser,
               UserName,
               UserLogin,
               0 AS Level,
               CAST('/' + CONVERT(VARCHAR, IdUser) + '/' AS VARCHAR(2000)) AS Path
        FROM Users u
            JOIN Seller s
                ON u.IdUser = s.IdUserSeller
        WHERE IdGenericStatus = 1
              AND IdUserSellerParent IS NULL
        UNION ALL
        SELECT u.IdUser,
               u.UserName,
               u.UserLogin,
               Level + 1,
               CAST(itms.Path + CONVERT(VARCHAR, ISNULL(u.IdUser, '')) + '/' AS VARCHAR(2000)) AS Path
        FROM Users u
            JOIN Seller s
                ON u.IdUser = s.IdUserSeller
            INNER JOIN items itms
                ON itms.IdUser = s.IdUserSellerParent
        WHERE IdGenericStatus = 1)
    SELECT IdUser,
           UserName,
           UserLogin,
           Level,
           Path
    INTO #SellerTree
    FROM items;

    INSERT INTO #SellerSubordinates
    SELECT IdUser
    FROM #SellerTree
    WHERE Path LIKE N'%/' + ISNULL(CONVERT(VARCHAR, @IdUserRequester), '0') + N'/%'
          AND @IdUserSeller = 0;


    --------------------------------------------------------------------------
    CREATE TABLE #TempAgents
    (
        IdAgent INT,
        IdCountry INT,
        IdCountryCurrency INT
    );
    CREATE TABLE #Temp
    (
        Id INT IDENTITY(1, 1),
        IdAgent INT,
        IdCountry INT,
        IdCountryCurrency INT,
        AgentName NVARCHAR(MAX),
        AgentCode NVARCHAR(MAX),
        IdSalesRep INT,
        SalesRep NVARCHAR(MAX),
        NumTrans INT,
        NumCancel INT,
        NumNet INT,
        AmountTrans MONEY,
        AmountCancel MONEY,
        AmountNet MONEY,
        CogsTrans MONEY,
        CogsCancel MONEY,
        CogsNet MONEY,
        FxResult MONEY,
        IncomeFee MONEY,
        AgentcommissionMonthly MONEY,
        AgentcommissionRetain MONEY,
        FxFee MONEY,
        FxFeeM MONEY,
        FxFeeR MONEY,
        Result MONEY,
        OtherCharges MONEY,
        OtherChargesD MONEY,
        OtherChargesC MONEY,
        NetResult MONEY,
        UnclaimedNumTrans INT,
        UnclaimedAmount MONEY,
        UnclaimedCOGS MONEY,
        BankCommission FLOAT,
        PayerCommission MONEY,
        FeeCanR MONEY,
        CashDiscount MONEY,
        NetFee MONEY,
        DCTran INT,
        MerchantFee MONEY
    );
    IF @Type = 1
    BEGIN
        --#2
        INSERT INTO #TempAgents
        (
            IdAgent,
            IdCountry,
            IdCountryCurrency
        )
        SELECT DISTINCT
               t.IdAgent,
               0 IdCountry,
               0 IdCountryCurrency
        FROM [dbo].[Transfer] AS t
        WHERE IdCountryCurrency = CASE
                                      WHEN @IdCountryCurrency = 0 THEN
                                          IdCountryCurrency
                                      ELSE
                                          @IdCountryCurrency
                                  END
              AND DateOfTransfer > @StartDate
              AND DateOfTransfer < @EndDate
              OR
              (
                  DateStatusChange > @StartDate
                  AND DateStatusChange < @EndDate
                  AND IdStatus IN ( 31, 22, 27 )
              )
			    AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	AmountInDollars BETWEEN @Range1 AND @Range2
				)
        UNION
        SELECT t.IdAgent,
               0 IdCountry,
               0 IdCountryCurrency
        FROM TransferClosed AS t 
        WHERE IdCountryCurrency = CASE
                                      WHEN @IdCountryCurrency = 0 THEN
                                          IdCountryCurrency
                                      ELSE
                                          @IdCountryCurrency
                                  END
              AND DateOfTransfer > @StartDate
              AND DateOfTransfer < @EndDate
              OR
              (
                  DateStatusChange > @StartDate
                  AND DateStatusChange < @EndDate
                  AND IdStatus IN ( 31, 22 )
              )
			  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	T.AmountInDollars BETWEEN @Range1 AND @Range2
				)
        UNION

        --#2
        SELECT ab.IdAgent,
               0 IdCountry,
               0 IdCountryCurrency
        FROM
            --agentbalance  ab With (Nolock, index(ix2_agentbalance))
            AgentBalance ab --#1
            JOIN AgentOtherCharge oc
                ON ab.IdAgentBalance = oc.IdAgentBalance
                   AND oc.IdOtherChargesMemo IN ( 6, 9, 13, 19, 4, 5, 11, 12, 16, 17, 18, 24, 25 )
        WHERE ab.DateOfMovement >= @StartDate
              AND ab.DateOfMovement < @EndDate
              AND
              (
                  ab.TypeOfMovement = 'CGO'
                  OR ab.TypeOfMovement = 'DEBT'
              )
        UNION
        SELECT AC.IdAgent,
               0 IdCountry,
               0 IdCountryCurrency
        FROM AgentCollectionDetail o
            INNER JOIN AgentCollection AC
                ON AC.IdAgentCollection = o.IdAgentCollection
        WHERE o.DateofLastChange >= @StartDate
              AND o.DateofLastChange < @EndDate
        UNION
        SELECT SC.IdAgent,
               0 IdCountry,
               0 IdCountryCurrency
        FROM [dbo].[SpecialCommissionBalance] SC
        WHERE SC.[DateOfApplication] >= @StartDate
              AND SC.[DateOfApplication] < @EndDate;
    END;
    IF @Type = 2
    BEGIN
        --#2
        INSERT INTO #TempAgents
        (
            IdAgent,
            IdCountry,
            IdCountryCurrency
        )
        SELECT DISTINCT
               t.IdAgent,
               IdCountry,
               0 IdCountryCurrency
        FROM [dbo].[Transfer] AS t --     
            JOIN CountryCurrency cc
                ON t.IdCountryCurrency = cc.IdCountryCurrency
        WHERE t.IdCountryCurrency = CASE
                                        WHEN @IdCountryCurrency = 0 THEN
                                            t.IdCountryCurrency
                                        ELSE
                                            @IdCountryCurrency
                                    END
              AND DateOfTransfer > @StartDate
              AND DateOfTransfer < @EndDate
              OR
              (
                  DateStatusChange > @StartDate
                  AND DateStatusChange < @EndDate
                  AND IdStatus IN ( 31, 22, 27 )
              )
			  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	AmountInDollars BETWEEN @Range1 AND @Range2
				)
        UNION
        SELECT t.IdAgent,
               IdCountry,
               0 IdCountryCurrency
        FROM [dbo].[TransferClosed] t 
        WHERE IdCountryCurrency = CASE
                                      WHEN @IdCountryCurrency = 0 THEN
                                          IdCountryCurrency
                                      ELSE
                                          @IdCountryCurrency
                                  END
              AND DateOfTransfer > @StartDate
              AND DateOfTransfer < @EndDate
              OR
              (
                  DateStatusChange > @StartDate
                  AND DateStatusChange < @EndDate
                  AND IdStatus IN ( 31, 22 )
              )
			  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	AmountInDollars BETWEEN @Range1 AND @Range2
				)
			  ;

    END;
    IF @Type = 3
    BEGIN
        --#2
        INSERT INTO #TempAgents
        (
            IdAgent,
            IdCountry,
            IdCountryCurrency
        )
        SELECT DISTINCT
               t.IdAgent,
               0 IdCountry,
               t.IdCountryCurrency
        FROM [dbo].[Transfer] t --     
            JOIN CountryCurrency cc /* */
                ON t.IdCountryCurrency = cc.IdCountryCurrency
        WHERE t.IdCountryCurrency = CASE
                                        WHEN @IdCountryCurrency = 0 THEN
                                            t.IdCountryCurrency
                                        ELSE
                                            @IdCountryCurrency
                                    END
              AND DateOfTransfer > @StartDate
              AND DateOfTransfer < @EndDate
              OR
              (
                  DateStatusChange > @StartDate
                  AND DateStatusChange < @EndDate
                  AND IdStatus IN ( 31, 22, 27 )
              )
			 AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	AmountInDollars BETWEEN @Range1 AND @Range2
				)
        UNION
        SELECT t.IdAgent,
               0 IdCountry,
               t.IdCountryCurrency
        FROM [dbo].TransferClosed t --WITH (nolock,INDEX(ixDateOfTransfer))
        WHERE IdCountryCurrency = CASE
                                      WHEN @IdCountryCurrency = 0 THEN
                                          IdCountryCurrency
                                      ELSE
                                          @IdCountryCurrency
                                  END
              AND DateOfTransfer > @StartDate
              AND DateOfTransfer < @EndDate
              OR
              (
                  DateStatusChange > @StartDate
                  AND DateStatusChange < @EndDate
                  AND IdStatus IN ( 31, 22 )
              )
			AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	AmountInDollars BETWEEN @Range1 AND @Range2
				)			  
			  ;

    END;
    INSERT INTO #Temp
    (
        IdAgent,
        IdCountry,
        IdCountryCurrency,
        AgentName,
        AgentCode,
        SalesRep,
        IdSalesRep
    )
    SELECT T.IdAgent,
           T.IdCountry,
           T.IdCountryCurrency,
           A.AgentName,
           A.AgentCode,
           ISNULL(UserName, ''),
           A.IdUserSeller
    FROM #TempAgents T
        INNER JOIN Agent A
            ON (A.IdAgent = T.IdAgent)
               AND A.AgentState = ISNULL(@State, A.AgentState)
        LEFT JOIN Users u
            ON u.IdUser = A.IdUserSeller
    WHERE @IsAllSeller = 1
          OR
          (
              A.IdUserSeller = @IdUserSeller
              OR A.IdUserSeller IN
                 (
                     SELECT IdSeller FROM #SellerSubordinates
                 )
          );

    --SELECT * FROM #Temp  

    ------------------------------Tranfer operation--------------------------------------------------
    WITH Transfers
    AS (SELECT t.IdAgent,
               IdCountry,
               t.IdCountryCurrency,
               t.IdGateway,
               t.IdPayer,
               t.IdPaymentType,
               AmountInDollars,
               ReferenceExRate,
               ExRate,
               Fee,
               TotalAmountToCorporate,
               AgentCommission,
               ModifierCommissionSlider,
               ModifierExchangeRateSlider,
               IdAgentCollectType,
               CAST(DATEADD(dd, - (DAY(DateOfTransfer) - 1), DateOfTransfer) AS DATE) DateOfCommission,
               CASE
                   WHEN DateOfTransfer <= '09/10/2014 16:14' THEN
                       CASE
                           WHEN t.IdAgentPaymentSchema = 1
                                AND Fee + AmountInDollars = TotalAmountToCorporate THEN
                               1
                           ELSE
                               2
                       END
                   ELSE
                       t.IdAgentPaymentSchema
               END IdAgentPaymentSchema,
               IdAgentBankDeposit,
               t.Discount,
               t.OperationFee,
               t.IdPaymentMethod,
               t.IdTransfer,
               CASE
                   WHEN pt.IdPosTransfer IS NOT NULL
                        AND t.IdPaymentMethod = 2 THEN
                       1
                   ELSE
                       0
               END SuccessfulCardPayment
        FROM [dbo].[Transfer] t
            JOIN Agent a
                ON t.IdAgent = a.IdAgent
            JOIN CountryCurrency cc
                ON t.IdCountryCurrency = cc.IdCountryCurrency
            LEFT JOIN PosTransfer pt
                ON pt.IdTransfer = t.IdTransfer
        WHERE t.IdCountryCurrency = CASE
                                        WHEN @IdCountryCurrency = 0 THEN
                                            t.IdCountryCurrency
                                        ELSE
                                            @IdCountryCurrency
                                    END
              AND DateOfTransfer > @StartDate
              AND DateOfTransfer < @EndDate
			  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	AmountInDollars BETWEEN @Range1 AND @Range2
				)
			)
    SELECT *
    INTO #temp1a
    FROM Transfers
    WHERE IdAgent IN
          (
              SELECT IdAgent FROM #Temp
          );


    --SELECT * FROM #temp1a
    SELECT DISTINCT
           t.IdAgent,
           IIF(@Type = 2, IdCountry, 0) AS IdCountry,
           IIF(@Type = 3, t.IdCountryCurrency, 0) AS IdCountryCurrency,
           COUNT(1) AS NumTrans1,
           SUM((AmountInDollars * ExRate) / ReferenceExRate) AS CogsTrans1,
           SUM(AmountInDollars) AS AmountTrans1,
           SUM(ROUND(((ReferenceExRate - ExRate) * AmountInDollars) / ReferenceExRate, 2)) AS FxResult1,
           SUM(IIF(IdAgentPaymentSchema = 1, AgentCommission, 0)) AS AgentcommissionMonthly1,
           SUM(IIF(IdAgentPaymentSchema = 2, AgentCommission, 0)) AS AgentcommissionRetain1,
           SUM(Fee) AS IncomeFee1,
           SUM(ModifierCommissionSlider + ModifierExchangeRateSlider) AS FxFee1,
           SUM(IIF(IdAgentPaymentSchema = 1, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeM1,
           SUM(IIF(IdAgentPaymentSchema = 2, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeR1,
           SUM(IIF(IdAgentBankDeposit IN ( 43, 46, 42 ), 0, ISNULL(AmountInDollars * FactorNew, 0))) AS BankCommission1,
           SUM(ISNULL(CommissionNew, 0)) AS PayerCommission1,
           SUM(IIF(t.SuccessfulCardPayment = 1, 1, 0)) AS DCTr1,
           SUM(IIF(t.SuccessfulCardPayment = 1, (t.OperationFee - t.Discount), 0)) AS MerchantFee1,
           SUM(t.Discount) AS CashDiscount1,
           SUM(t.Fee - t.Discount) AS NetFee1
    INTO #temp1
    FROM #temp1a t
        LEFT JOIN #bankcommission b
            ON b.DateOfBankCommission = DateOfCommission
        LEFT JOIN #payercommission p
            ON p.DateOfPayerConfigCommission = DateOfCommission
               AND p.IdGateway = t.IdGateway
               AND p.IdPayer = t.IdPayer
               AND p.IdPaymentType = t.IdPaymentType
               AND t.IdCountryCurrency = p.IdCountryCurrency
    GROUP BY t.IdAgent,
             IIF(@Type = 2, IdCountry, 0),
             IIF(@Type = 3, t.IdCountryCurrency, 0);

    --#5

    --SELECT * FROM #temp1


    ------------------------------Tranfer Closed operation--------------------------------------------------

    -- First part of the query
    SELECT t.IdAgent,
           IdCountry,
           t.IdCountryCurrency,
           t.IdGateway,
           t.IdPayer,
           t.IdPaymentType,
           AmountInDollars,
           ReferenceExRate,
           ExRate,
           Fee,
           TotalAmountToCorporate,
           AgentCommission,
           ModifierCommissionSlider,
           ModifierExchangeRateSlider,
           IdAgentCollectType,
           CAST(DATEADD(dd, - (DAY(DateOfTransfer) - 1), DateOfTransfer) AS DATE) DateOfCommission,
           IIF(DateOfTransfer <= '09/10/2014 16:14',
               IIF(t.IdAgentPaymentSchema = 1 AND Fee + AmountInDollars = TotalAmountToCorporate, 1, 2),
               t.IdAgentPaymentSchema) AS IdAgentPaymentSchema,
           IdAgentBankDeposit,
           t.Discount,
           t.OperationFee,
           t.IdPaymentMethod,
           IIF(pt.IdPosTransfer IS NOT NULL AND t.IdPaymentMethod = 2, 1, 0) AS SuccessfulCardPayment
    INTO #temp2a
    FROM TransferClosed t
        JOIN Agent a
            ON t.IdAgent = a.IdAgent
        LEFT JOIN PosTransfer pt
            ON pt.IdTransferClosed = t.IdTransferClosed
    WHERE IdCountryCurrency = IIF(@IdCountryCurrency = 0, IdCountryCurrency, @IdCountryCurrency)
          AND DateOfTransfer > @StartDate
          AND DateOfTransfer < @EndDate
          AND t.IdAgent IN
              (
                  SELECT IdAgent FROM #TempAgents
              )
		  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	T.AmountInDollars BETWEEN @Range1 AND @Range2
				)			  
			  ;

    -- Second part of the query
    SELECT DISTINCT
           t.IdAgent,
           IIF(@Type = 2, IdCountry, 0) AS IdCountry,
           IIF(@Type = 3, t.IdCountryCurrency, 0) AS IdCountryCurrency,
           COUNT(1) AS NumTrans2,
           SUM((AmountInDollars * ExRate) / ReferenceExRate) AS CogsTrans2,
           SUM(AmountInDollars) AS AmountTrans2,
           SUM(ROUND(((ReferenceExRate - ExRate) * AmountInDollars) / ReferenceExRate, 2)) AS FxResult2,
           SUM(IIF(IdAgentPaymentSchema = 1, AgentCommission, 0)) AS AgentcommissionMonthly2,
           SUM(IIF(IdAgentPaymentSchema = 2, AgentCommission, 0)) AS AgentcommissionRetain2,
           SUM(Fee) AS IncomeFee2,
           SUM(ModifierCommissionSlider + ModifierExchangeRateSlider) AS FxFee2,
           SUM(IIF(IdAgentPaymentSchema = 1, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeM2,
           SUM(IIF(IdAgentPaymentSchema = 2, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeR2,
           SUM(IIF(IdAgentBankDeposit IN ( 43, 46, 42 ), 0, ISNULL(AmountInDollars * FactorNew, 0))) AS BankCommission2,
           SUM(ISNULL(CommissionNew, 0)) AS PayerCommission2,
           SUM(IIF(t.SuccessfulCardPayment = 1, 1, 0)) AS DCTr2,
           SUM(IIF(t.SuccessfulCardPayment = 1, (t.OperationFee - t.Discount), 0)) AS MerchantFee2,
           SUM(t.Discount) AS CashDiscount2,
           SUM(t.Fee - t.Discount) AS NetFee2
    INTO #temp2
    FROM #temp2a t
        LEFT JOIN #bankcommission b
            ON b.DateOfBankCommission = DateOfCommission
        LEFT JOIN #payercommission p
            ON p.DateOfPayerConfigCommission = DateOfCommission
               AND p.IdGateway = t.IdGateway
               AND p.IdPayer = t.IdPayer
               AND p.IdPaymentType = t.IdPaymentType
               AND t.IdCountryCurrency = p.IdCountryCurrency
    GROUP BY t.IdAgent,
             IIF(@Type = 2, IdCountry, 0),
             IIF(@Type = 3, t.IdCountryCurrency, 0);



    ------------------------------Tranfer Rejected --------------------------------------------------
    -- First part of the query
    SELECT t.IdAgent,
           IdCountry,
           t.IdCountryCurrency,
           t.IdGateway,
           t.IdPayer,
           t.IdPaymentType,
           AmountInDollars,
           ReferenceExRate,
           ExRate,
           Fee,
           TotalAmountToCorporate,
           AgentCommission,
           ModifierCommissionSlider,
           ModifierExchangeRateSlider,
           IdAgentCollectType,
           CAST(DATEADD(dd, - (DAY(DateStatusChange) - 1), DateStatusChange) AS DATE) DateOfCommission,
           IIF(DateOfTransfer <= '09/10/2014 16:14',
               IIF(t.IdAgentPaymentSchema = 1 AND Fee + AmountInDollars = TotalAmountToCorporate, 1, 2),
               t.IdAgentPaymentSchema) AS IdAgentPaymentSchema,
           IdAgentBankDeposit,
           t.Discount,
           t.OperationFee,
           t.IdPaymentMethod
    INTO #temp3a
    FROM Transfer t
        JOIN Agent a
            ON t.IdAgent = a.IdAgent
        JOIN CountryCurrency cc
            ON t.IdCountryCurrency = cc.IdCountryCurrency
    WHERE t.IdCountryCurrency = IIF(@IdCountryCurrency = 0, t.IdCountryCurrency, @IdCountryCurrency)
          AND DateStatusChange > @StartDate
          AND DateStatusChange < @EndDate
          AND t.IdAgent IN
              (
                  SELECT IdAgent FROM #TempAgents
              )
          AND IdStatus = 31
		  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	T.AmountInDollars BETWEEN @Range1 AND @Range2
				)	
		  
		  ;

    -- Second part of the query
    SELECT DISTINCT
           IdAgent,
           IIF(@Type = 2, IdCountry, 0) AS IdCountry,
           IIF(@Type = 3, t.IdCountryCurrency, 0) AS IdCountryCurrency,
           COUNT(1) AS NumTransRej1,
           SUM((AmountInDollars * ExRate) / ReferenceExRate) AS CogsRej1,
           SUM(AmountInDollars) AS AmountTransRej1,
           SUM(ROUND(((ReferenceExRate - ExRate) * AmountInDollars) / ReferenceExRate, 2)) AS FxResultRej1,
           SUM(IIF(IdAgentPaymentSchema = 1, AgentCommission, 0)) AS AgentcommissionMonthlyRej1,
           SUM(IIF(IdAgentPaymentSchema = 2, AgentCommission, 0)) AS AgentcommissionRetainRej1,
           SUM(Fee) AS IncomeFeeRej1,
           SUM(ModifierCommissionSlider + ModifierExchangeRateSlider) AS FxFeeRej1,
           SUM(IIF(IdAgentPaymentSchema = 1, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeRejM1,
           SUM(IIF(IdAgentPaymentSchema = 2, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeRejR1,
           SUM(IIF(IdAgentBankDeposit IN ( 43, 46, 42 ), 0, ISNULL(AmountInDollars * FactorNew, 0))) AS BankCommission3,
           SUM(ISNULL(CommissionNew, 0)) AS PayerCommission3,
           SUM(IIF(t.IdPaymentMethod = 2, 1, 0)) AS DCTr3,
           SUM(t.OperationFee - t.Discount) AS MerchantFee3,
           SUM(t.Discount) AS CashDiscount3,
           SUM(t.Fee - t.Discount) AS NetFee3
    INTO #temp3
    FROM #temp3a t
        LEFT JOIN #bankcommission b
            ON b.DateOfBankCommission = DateOfCommission
        LEFT JOIN #payercommission p
            ON p.DateOfPayerConfigCommission = DateOfCommission
               AND p.IdGateway = t.IdGateway
               AND p.IdPayer = t.IdPayer
               AND p.IdPaymentType = t.IdPaymentType
               AND t.IdCountryCurrency = p.IdCountryCurrency
    GROUP BY t.IdAgent,
             IIF(@Type = 2, IdCountry, 0),
             IIF(@Type = 3, t.IdCountryCurrency, 0);

    ------------------------------Tranfer Closed Rejected --------------------------------------------------
    SELECT t.IdAgent,
           IdCountry,
           t.IdCountryCurrency,
           t.IdGateway,
           t.IdPayer,
           t.IdPaymentType,
           AmountInDollars,
           ReferenceExRate,
           ExRate,
           Fee,
           TotalAmountToCorporate,
           AgentCommission,
           ModifierCommissionSlider,
           ModifierExchangeRateSlider,
           IdAgentCollectType,
           CAST(DATEADD(dd, - (DAY(DateStatusChange) - 1), DateStatusChange) AS DATE) DateOfCommission,
           IIF(DateOfTransfer <= '09/10/2014 16:14',
               IIF(t.IdAgentPaymentSchema = 1 AND Fee + AmountInDollars = TotalAmountToCorporate, 1, 2),
               t.IdAgentPaymentSchema) IdAgentPaymentSchema,
           IdAgentBankDeposit,
           t.Discount,
           t.OperationFee,
           t.IdPaymentMethod
    INTO #temp4a
    FROM TransferClosed t
        JOIN Agent a
            ON t.IdAgent = a.IdAgent
    WHERE IdCountryCurrency = IIF(@IdCountryCurrency = 0, IdCountryCurrency, @IdCountryCurrency)
          AND DateStatusChange > @StartDate
          AND DateStatusChange < @EndDate
          AND t.IdAgent IN
              (
                  SELECT IdAgent FROM #TempAgents
              )
          AND IdStatus = 31
		  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	T.AmountInDollars BETWEEN @Range1 AND @Range2
				)	
		  ;

    SELECT DISTINCT
           IdAgent,
           IIF(@Type = 2, IdCountry, 0) IdCountry,
           IIF(@Type = 3, t.IdCountryCurrency, 0) IdCountryCurrency,
           COUNT(1) AS NumTrans2Rej,
           SUM((AmountInDollars * ExRate) / ReferenceExRate) AS CogsRej2,
           SUM(AmountInDollars) AS AmountTrans2Rej,
           SUM(ROUND(((ReferenceExRate - ExRate) * AmountInDollars) / ReferenceExRate, 2)) AS FxResult2Rej,
           SUM(IIF(IdAgentPaymentSchema = 1, AgentCommission, 0)) AS AgentcommissionMonthly2Rej,
           SUM(IIF(IdAgentPaymentSchema = 2, AgentCommission, 0)) AS AgentcommissionRetain2Rej,
           SUM(Fee) AS IncomeFee2Rej,
           SUM(ModifierCommissionSlider + ModifierExchangeRateSlider) AS FxFee2Rej,
           SUM(IIF(IdAgentPaymentSchema = 1, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeM2Rej,
           SUM(IIF(IdAgentPaymentSchema = 2, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeR2Rej,
           SUM(IIF(IdAgentBankDeposit IN ( 43, 46, 42 ), 0, ISNULL(AmountInDollars * FactorNew, 0))) AS BankCommission4,
           SUM(ISNULL(CommissionNew, 0)) AS PayerCommission4,
           SUM(IIF(t.IdPaymentMethod = 2, 1, 0)) AS DCTr4,
           SUM(t.OperationFee - t.Discount) AS MerchantFee4,
           SUM(t.Discount) AS CashDiscount4,
           SUM(t.Fee - t.Discount) AS NetFee4
    INTO #temp4
    FROM #temp4a t
        LEFT JOIN #bankcommission b
            ON b.DateOfBankCommission = DateOfCommission
        LEFT JOIN #payercommission p
            ON p.DateOfPayerConfigCommission = DateOfCommission
               AND p.IdGateway = t.IdGateway
               AND p.IdPayer = t.IdPayer
               AND p.IdPaymentType = t.IdPaymentType
               AND t.IdCountryCurrency = p.IdCountryCurrency
    GROUP BY t.IdAgent,
             IIF(@Type = 2, IdCountry, 0),
             IIF(@Type = 3, t.IdCountryCurrency, 0);

    ------------------------------Tranfer Cancel --------------------------------------------------
    SELECT T.IdTransfer,
           T.IdAgent,
           IIF(@Type = 2, IdCountry, 0) AS IdCountry,
           IIF(@Type = 3, T.IdCountryCurrency, 0) AS IdCountryCurrency,
           T.IdGateway,
           T.IdPayer,
           T.IdPaymentType,
           AmountInDollars,
           CASE
               WHEN DATEDIFF(MINUTE, T.DateOfTransfer, T.DateStatusChange) <= 30
                    OR TN.IdTransfer IS NOT NULL THEN
                   0
               ELSE
                   IIF(rc.ReturnAllComission = 1, 0, AgentCommissionExtra)
           END AgentCommissionExtra,
           ReferenceExRate,
           ExRate,
           Fee,
           TotalAmountToCorporate,
           AgentCommission,
           ModifierCommissionSlider,
           ModifierExchangeRateSlider,
           IdAgentCollectType,
           CAST(DATEADD(dd, - (DAY(DateStatusChange) - 1), DateStatusChange) AS DATE) DateOfCommission,
           CASE
               WHEN DateOfTransfer <= '09/10/2014 16:14' THEN
                   IIF(T.IdAgentPaymentSchema = 1 AND Fee + AmountInDollars = TotalAmountToCorporate, 1, 2)
               ELSE
                   T.IdAgentPaymentSchema
           END IdAgentPaymentSchema,
           IdAgentBankDeposit,
           T.Discount,
           T.OperationFee,
           T.IdPaymentMethod
    INTO #temp5a
    FROM Transfer T
        JOIN Agent a
            ON T.IdAgent = a.IdAgent
        JOIN CountryCurrency cc
            ON T.IdCountryCurrency = cc.IdCountryCurrency
        LEFT JOIN TransferNotAllowedResend TN
            ON TN.IdTransfer = T.IdTransfer
        LEFT JOIN ReasonForCancel rc
            ON T.IdReasonForCancel = rc.IdReasonForCancel
    WHERE T.IdCountryCurrency = IIF(@IdCountryCurrency = 0, T.IdCountryCurrency, @IdCountryCurrency)
          AND DateStatusChange > @StartDate
          AND DateStatusChange < @EndDate
          AND T.IdAgent IN
              (
                  SELECT IdAgent FROM #TempAgents
              )
          AND IdStatus = 22
		  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	T.AmountInDollars BETWEEN @Range1 AND @Range2
				)			  
		  ;

    SELECT DISTINCT
           IdAgent,
           IIF(@Type = 2, IdCountry, 0) AS IdCountry,
           IIF(@Type = 3, T.IdCountryCurrency, 0) AS IdCountryCurrency,
           SUM((AmountInDollars * ExRate) / ReferenceExRate) AS CogsCancel1,
           COUNT(1) AS NumCancel1,
           SUM(AmountInDollars - IIF(IdAgentPaymentSchema = 1, 0, AgentCommissionExtra)) AS AmountCancel1,
           SUM(ROUND(((ReferenceExRate - ExRate) * AmountInDollars) / ReferenceExRate, 2)) AS FxResultCancel1,
           SUM(IIF(IdAgentPaymentSchema = 1, AgentCommission, 0)) AS AgentcommissionMonthlyCan1,
           SUM(IIF(IdAgentPaymentSchema = 2, 0, 0)) AS AgentcommissionRetainCan1,
           SUM(ModifierCommissionSlider + ModifierExchangeRateSlider) AS FxFeeCan1,
           SUM(IIF(IdAgentPaymentSchema = 1, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeCanM1,
           SUM(IIF(IdAgentPaymentSchema = 2, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeCanR1,
           SUM(Fee) AS IncomeFeeCan1,
           SUM(IIF(TA.IdTransfer IS NULL, 0, Fee)) AS IncomeFeeCancelLikeReject1,
           SUM(IIF(TA.IdTransfer IS NULL, 0, (IIF(Fee + AmountInDollars <> TotalAmountToCorporate, AgentCommission, 0)))) AS AgentcommissionRetainCancelLikeReject1,
           SUM(IIF(IdAgentBankDeposit IN ( 43, 46, 42 ), 0, ISNULL(AmountInDollars * FactorNew, 0))) AS BankCommission5,
           SUM(ISNULL(CommissionNew, 0)) AS PayerCommission5,
           SUM(IIF(T.IdPaymentMethod = 2, 1, 0)) AS DCTr5,
           SUM(T.OperationFee - T.Discount) AS MerchantFee5,
           SUM(T.Discount) AS CashDiscount5,
           SUM(T.Fee - T.Discount) AS NetFee5
    INTO #temp5
    FROM #temp5a T
        LEFT JOIN dbo.TransferNotAllowedResend TA
            ON T.IdTransfer = TA.IdTransfer
        LEFT JOIN #bankcommission b
            ON b.DateOfBankCommission = DateOfCommission
        LEFT JOIN #payercommission p
            ON p.DateOfPayerConfigCommission = DateOfCommission
               AND p.IdGateway = T.IdGateway
               AND p.IdPayer = T.IdPayer
               AND p.IdPaymentType = T.IdPaymentType
               AND T.IdCountryCurrency = p.IdCountryCurrency
    GROUP BY T.IdAgent,
             IIF(@Type = 2, IdCountry, 0),
             IIF(@Type = 3, T.IdCountryCurrency, 0);

    ------------------------------Tranfer Closed Cancel --------------------------------------------------
    SELECT T.IdTransferClosed,
           T.IdAgent,
           IdCountry,
           T.IdCountryCurrency,
           T.IdGateway,
           T.IdPayer,
           T.IdPaymentType,
           AmountInDollars,
           CASE
               WHEN DATEDIFF(MINUTE, T.DateOfTransfer, T.DateStatusChange) <= 30 THEN
                   0
               WHEN TN.IdTransfer IS NOT NULL THEN
                   0
               ELSE
                   IIF(rc.ReturnAllComission = 1, 0, AgentCommissionExtra)
           END AgentCommissionExtra,
           ReferenceExRate,
           ExRate,
           Fee,
           TotalAmountToCorporate,
           AgentCommission,
           ModifierCommissionSlider,
           ModifierExchangeRateSlider,
           IdAgentCollectType,
           CAST(DATEADD(dd, - (DAY(DateStatusChange) - 1), DateStatusChange) AS DATE) DateOfCommission,
           IIF(DateOfTransfer <= '09/10/2014 16:14',
               IIF(T.IdAgentPaymentSchema = 1 AND Fee + AmountInDollars = TotalAmountToCorporate, 1, 2),
               T.IdAgentPaymentSchema) IdAgentPaymentSchema,
           IdAgentBankDeposit,
           T.Discount,
           T.OperationFee,
           T.IdPaymentMethod
    INTO #temp6a
    FROM TransferClosed T
        JOIN Agent a
            ON T.IdAgent = a.IdAgent
        LEFT JOIN TransferNotAllowedResend TN
            ON TN.IdTransfer = T.IdTransferClosed
        LEFT JOIN ReasonForCancel rc
            ON T.IdReasonForCancel = rc.IdReasonForCancel
    WHERE IdCountryCurrency = IIF(@IdCountryCurrency = 0, IdCountryCurrency, @IdCountryCurrency)
          AND DateStatusChange > @StartDate
          AND DateStatusChange < @EndDate
          AND T.IdAgent IN
              (
                  SELECT IdAgent FROM #TempAgents
              )
          AND IdStatus = 22
		  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	T.AmountInDollars BETWEEN @Range1 AND @Range2
				)	
		  ;

    SELECT DISTINCT
           T.IdAgent,
           IIF(@Type = 2, IdCountry, 0) IdCountry,
           IIF(@Type = 3, T.IdCountryCurrency, 0) IdCountryCurrency,
           SUM((AmountInDollars * ExRate) / ReferenceExRate) AS CogsCancel2,
           COUNT(1) AS NumCancel2,
           SUM(AmountInDollars - IIF(IdAgentPaymentSchema = 1, 0, AgentCommissionExtra)) AS AmountCancel2,
           SUM(ROUND(((ReferenceExRate - ExRate) * AmountInDollars) / ReferenceExRate, 2)) AS FxResultCancel2,
           SUM(IIF(IdAgentPaymentSchema = 1, AgentCommission, 0)) AS AgentcommissionMonthlyCan2,
           SUM(IIF(IdAgentPaymentSchema = 2, 0, 0)) AS AgentcommissionRetainCan2,
           SUM(ModifierCommissionSlider + ModifierExchangeRateSlider) AS FxFeeCan2,
           SUM(IIF(IdAgentPaymentSchema = 1, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeCanM2,
           SUM(IIF(IdAgentPaymentSchema = 2, ModifierCommissionSlider + ModifierExchangeRateSlider, 0)) AS FxFeeCanR2,
           SUM(Fee) AS IncomeFeeCan2,
           SUM(IIF(TA.IdTransfer IS NULL, 0, Fee)) AS IncomeFeeCancelLikeReject2,
           SUM(IIF(TA.IdTransfer IS NULL, 0, IIF(Fee + AmountInDollars <> TotalAmountToCorporate, AgentCommission, 0))) AS AgentcommissionRetainCancelLikeReject2,
           SUM(IIF(IdAgentBankDeposit IN ( 43, 46, 42 ), 0, ISNULL(AmountInDollars * FactorNew, 0))) AS BankCommission6,
           SUM(ISNULL(CommissionNew, 0)) AS PayerCommission6,
           SUM(IIF(T.IdPaymentMethod = 2, 1, 0)) AS DCTr6,
           SUM(T.OperationFee - T.Discount) AS MerchantFee6,
           SUM(T.Discount) AS CashDiscount6,
           SUM(T.Fee - T.Discount) AS NetFee6
    INTO #temp6
    FROM #temp6a T
        LEFT JOIN dbo.TransferNotAllowedResend TA
            ON T.IdTransferClosed = TA.IdTransfer
        LEFT JOIN #bankcommission b
            ON b.DateOfBankCommission = DateOfCommission
        LEFT JOIN #payercommission p
            ON p.DateOfPayerConfigCommission = DateOfCommission
               AND p.IdGateway = T.IdGateway
               AND p.IdPayer = T.IdPayer
               AND p.IdPaymentType = T.IdPaymentType
               AND T.IdCountryCurrency = p.IdCountryCurrency
    GROUP BY T.IdAgent,
             IIF(@Type = 2, IdCountry, 0),
             IIF(@Type = 3, T.IdCountryCurrency, 0);



    ------------------------------Other Charges  --------------------------------------------------
    SELECT DISTINCT
           ab.IdAgent,
           SUM(IIF(ab.DebitOrCredit = 'Credit', ab.Amount, ab.Amount * (-1))) OVER (PARTITION BY ab.IdAgent) AS OtherCharges1,
           SUM(IIF(oc.IdOtherChargesMemo IN ( 6, 9, 13, 19 ),
                IIF(ab.DebitOrCredit = 'Credit', ab.Amount, ab.Amount * (-1)),
                0)
              ) OVER (PARTITION BY ab.IdAgent) AS OtherChargesC1,
           SUM(IIF(oc.IdOtherChargesMemo IN ( 4, 5, 11, 12, 16, 17, 18, 24, 25 ),
                   IIF(ab.DebitOrCredit != 'Credit', ab.Amount, ab.Amount * (-1)),
                   0)
              ) OVER (PARTITION BY ab.IdAgent) AS OtherChargesD1
    INTO #temp7
    FROM AgentBalance ab
        JOIN AgentOtherCharge oc
            ON ab.IdAgentBalance = oc.IdAgentBalance
               AND oc.IdOtherChargesMemo IN ( 6, 9, 13, 19, 4, 5, 11, 12, 16, 17, 18, 24, 25 )
    WHERE ab.DateOfMovement >= @StartDate
          AND ab.DateOfMovement < @EndDate
          AND
          (
              ab.TypeOfMovement = 'CGO'
              OR ab.TypeOfMovement = 'DEBT'
          );

    SELECT DISTINCT
           AC.IdAgent,
           SUM(o.AmountToPay) OVER (PARTITION BY AC.IdAgent) AS OtherCharges2,
           SUM(IIF(o.AmountToPay > 0, o.AmountToPay, 0)) OVER (PARTITION BY AC.IdAgent) AS OtherChargesC2,
           SUM(IIF(o.AmountToPay < 0, o.AmountToPay * (-1), 0)) OVER (PARTITION BY AC.IdAgent) AS OtherChargesD2
    INTO #temp10
    FROM AgentCollectionDetail o
        INNER JOIN AgentCollection AC
            ON AC.IdAgentCollection = o.IdAgentCollection
    WHERE @ReportCorpType = 1
          AND o.DateofLastChange >= @StartDate
          AND o.DateofLastChange < @EndDate;


    ------------------------------Tranfer Unclaimed --------------------------------------------------
    SELECT DISTINCT
           IdAgent,
           IIF(@Type = 2, IdCountry, 0) AS IdCountry,
           IIF(@Type = 3, t.IdCountryCurrency, 0) AS IdCountryCurrency,
           SUM((AmountInDollars * ExRate) / ReferenceExRate) OVER (PARTITION BY t.IdAgent,
                                                                                IIF(@Type = 2, IdCountry, 0),
                                                                                IIF(@Type = 3, t.IdCountryCurrency, 0)
                                                                  ) AS UnclaimedCOGS1,
           COUNT(1) OVER (PARTITION BY t.IdAgent,
                                       IIF(@Type = 2, IdCountry, 0),
                                       IIF(@Type = 3, t.IdCountryCurrency, 0)
                         ) AS UnclaimedNumTrans1,
           SUM(AmountInDollars) OVER (PARTITION BY t.IdAgent,
                                                   IIF(@Type = 2, IdCountry, 0),
                                                   IIF(@Type = 3, t.IdCountryCurrency, 0)
                                     ) AS UnclaimedAmount1
    INTO #temp8
    FROM Transfer t
        JOIN CountryCurrency cc
            ON t.IdCountryCurrency = cc.IdCountryCurrency
    WHERE t.IdCountryCurrency = CASE
                                    WHEN @IdCountryCurrency = 0 THEN
                                        t.IdCountryCurrency
                                    ELSE
                                        @IdCountryCurrency
                                END
          AND DateStatusChange > @StartDate
          AND DateStatusChange < @EndDate
          AND IdStatus = 27
		  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	T.AmountInDollars BETWEEN @Range1 AND @Range2
				)	
		  ;


    ------------------------------Tranfer Closed Unclaimed --------------------------------------------------	

    SELECT DISTINCT
           IdAgent,
           IIF(@Type = 2, IdCountry, 0) AS IdCountry,
           IIF(@Type = 3, t.IdCountryCurrency, 0) AS IdCountryCurrency,
           SUM((AmountInDollars * ExRate) / ReferenceExRate) OVER (PARTITION BY t.IdAgent,
                                                                                IIF(@Type = 2, IdCountry, 0),
                                                                                IIF(@Type = 3, t.IdCountryCurrency, 0)
                                                                  ) AS UnclaimedCOGSClosed,
           COUNT(1) OVER (PARTITION BY t.IdAgent,
                                       IIF(@Type = 2, IdCountry, 0),
                                       IIF(@Type = 3, t.IdCountryCurrency, 0)
                         ) AS UnclaimedNumTransClosed,
           SUM(AmountInDollars) OVER (PARTITION BY t.IdAgent,
                                                   IIF(@Type = 2, IdCountry, 0),
                                                   IIF(@Type = 3, t.IdCountryCurrency, 0)
                                     ) AS UnclaimedAmountClosed
    INTO #temp9
    FROM TransferClosed t
    WHERE IdCountryCurrency = CASE
                                  WHEN @IdCountryCurrency = 0 THEN
                                      IdCountryCurrency
                                  ELSE
                                      @IdCountryCurrency
                              END
          AND DateStatusChange > @StartDate
          AND DateStatusChange < @EndDate
          AND IdStatus = 27
		  AND (
					(@Range1 IS NULL AND @Range2 IS NULL)
					OR	T.AmountInDollars BETWEEN @Range1 AND @Range2
				)	
		  ;

    ------------------------------Calculate OutPut --------------------------------------------------
    SELECT A.*,
           B.NumTrans1,
           B.AmountTrans1,
           B.FxResult1,
           B.AgentcommissionMonthly1,
           B.AgentcommissionRetain1,
           B.IncomeFee1,
           B.FxFee1,
           B.FxFeeM1,
           B.FxFeeR1,
           B.BankCommission1,
           B.PayerCommission1,
           B.CogsTrans1,
           B.DCTr1,
           B.MerchantFee1,
           B.CashDiscount1,
           B.NetFee1,
           C.NumTrans2,
           C.AmountTrans2,
           C.FxResult2,
           C.AgentcommissionMonthly2,
           C.AgentcommissionRetain2,
           C.IncomeFee2,
           C.FxFee2,
           C.FxFeeM2,
           C.FxFeeR2,
           C.BankCommission2,
           C.PayerCommission2,
           C.CogsTrans2,
           C.DCTr2,
           C.MerchantFee2,
           C.CashDiscount2,
           C.NetFee2,
           D.NumTransRej1,
           D.AmountTransRej1,
           D.FxResultRej1,
           D.AgentcommissionMonthlyRej1,
           D.AgentcommissionRetainRej1,
           D.IncomeFeeRej1,
           D.FxFeeRej1,
           D.FxFeeRejM1,
           D.FxFeeRejR1,
           D.BankCommission3,
           D.PayerCommission3,
           D.CogsRej1,
           D.DCTr3,
           D.MerchantFee3,
           D.CashDiscount3,
           D.NetFee3,
           E.NumTrans2Rej,
           E.AmountTrans2Rej,
           E.FxResult2Rej,
           E.AgentcommissionMonthly2Rej,
           E.AgentcommissionRetain2Rej,
           E.IncomeFee2Rej,
           E.FxFee2Rej,
           E.FxFeeM2Rej,
           E.FxFeeR2Rej,
           E.BankCommission4,
           E.PayerCommission4,
           E.CogsRej2,
           E.DCTr4,
           E.MerchantFee4,
           E.CashDiscount4,
           E.NetFee4,
           F.CogsCancel1,
           F.NumCancel1,
           F.AmountCancel1,
           F.FxResultCancel1,
           F.AgentcommissionMonthlyCan1,
           F.AgentcommissionRetainCan1,
           F.FxFeeCan1,
           F.FxFeeCanM1,
           F.FxFeeCanR1,
           F.IncomeFeeCan1,
           F.IncomeFeeCancelLikeReject1,
           F.AgentcommissionRetainCancelLikeReject1,
           F.BankCommission5,
           F.PayerCommission5,
           F.DCTr5,
           F.MerchantFee5,
           F.CashDiscount5,
           F.NetFee5,
           G.CogsCancel2,
           G.NumCancel2,
           G.AmountCancel2,
           G.FxResultCancel2,
           G.AgentcommissionMonthlyCan2,
           G.AgentcommissionRetainCan2,
           G.FxFeeCan2,
           G.FxFeeCanM2,
           G.FxFeeCanR2,
           G.IncomeFeeCan2,
           G.IncomeFeeCancelLikeReject2,
           G.AgentcommissionRetainCancelLikeReject2,
           G.BankCommission6,
           G.PayerCommission6,
           G.DCTr6,
           G.MerchantFee6,
           G.CashDiscount6,
           G.NetFee6,
           H.OtherCharges1,
           H.OtherChargesC1,
           H.OtherChargesD1,
           I.UnclaimedCOGS1,
           I.UnclaimedNumTrans1,
           I.UnclaimedAmount1,
           J.UnclaimedCOGSClosed,
           J.UnclaimedNumTransClosed,
           J.UnclaimedAmountClosed,
           K.OtherCharges2,
           K.OtherChargesC2,
           K.OtherChargesD2
    INTO #Result
    FROM #Temp A
        LEFT JOIN #temp1 B
            ON (A.IdAgent = B.IdAgent)
               AND (A.IdCountry = B.IdCountry)
               AND A.IdCountryCurrency = B.IdCountryCurrency
        LEFT JOIN #temp2 C
            ON (A.IdAgent = C.IdAgent)
               AND (A.IdCountry = C.IdCountry)
               AND A.IdCountryCurrency = C.IdCountryCurrency
        LEFT JOIN #temp3 D
            ON (A.IdAgent = D.IdAgent)
               AND (A.IdCountry = D.IdCountry)
               AND A.IdCountryCurrency = D.IdCountryCurrency
        LEFT JOIN #temp4 E
            ON (A.IdAgent = E.IdAgent)
               AND (A.IdCountry = E.IdCountry)
               AND A.IdCountryCurrency = E.IdCountryCurrency
        LEFT JOIN #temp5 F
            ON (A.IdAgent = F.IdAgent)
               AND (A.IdCountry = F.IdCountry)
               AND A.IdCountryCurrency = F.IdCountryCurrency
        LEFT JOIN #temp6 G
            ON (A.IdAgent = G.IdAgent)
               AND (A.IdCountry = G.IdCountry)
               AND A.IdCountryCurrency = G.IdCountryCurrency
        LEFT JOIN #temp7 H
            ON (A.IdAgent = H.IdAgent) --other charges
        LEFT JOIN #temp8 I
            ON (A.IdAgent = I.IdAgent)
               AND (A.IdCountry = I.IdCountry)
               AND A.IdCountryCurrency = I.IdCountryCurrency
        LEFT JOIN #temp9 J
            ON (A.IdAgent = J.IdAgent)
               AND (A.IdCountry = J.IdCountry)
               AND A.IdCountryCurrency = J.IdCountryCurrency
        LEFT JOIN #temp10 K
            ON (A.IdAgent = K.IdAgent); --other charges



    UPDATE #Result
    SET NumTrans = COALESCE(NumTrans1, 0) + COALESCE(NumTrans2, 0),
        NumCancel = COALESCE(NumCancel1, 0) + COALESCE(NumCancel2, 0) + COALESCE(NumTransRej1, 0)
                    + COALESCE(NumTrans2Rej, 0),
        AmountTrans = COALESCE(AmountTrans1, 0) + COALESCE(AmountTrans2, 0),
        AmountCancel = COALESCE(AmountCancel1, 0) + COALESCE(AmountCancel2, 0) + COALESCE(AmountTransRej1, 0)
                       + COALESCE(AmountTrans2Rej, 0),
        OtherCharges = COALESCE(OtherCharges1, 0) + COALESCE(OtherCharges2, 0),
        OtherChargesD = COALESCE(OtherChargesD1, 0) + COALESCE(OtherChargesD2, 0),
        OtherChargesC = COALESCE(OtherChargesC1, 0) + COALESCE(OtherChargesC2, 0),
        CogsCancel = COALESCE(CogsCancel1, 0) + COALESCE(CogsCancel2, 0) + COALESCE(CogsRej1, 0)
                     + COALESCE(CogsRej2, 0),
        FxResult = COALESCE(FxResult1, 0) + COALESCE(FxResult2, 0) - COALESCE(FxResultRej1, 0)
                   - COALESCE(FxResult2Rej, 0) - COALESCE(FxResultCancel1, 0) - COALESCE(FxResultCancel2, 0),
        AgentcommissionMonthly = COALESCE(AgentcommissionMonthly1, 0) + COALESCE(AgentcommissionMonthly2, 0)
                                 - COALESCE(AgentcommissionMonthlyRej1, 0) - COALESCE(AgentcommissionMonthly2Rej, 0)
                                 - COALESCE(AgentcommissionMonthlyCan1, 0) - COALESCE(AgentcommissionMonthlyCan2, 0),
        AgentcommissionRetain = COALESCE(AgentcommissionRetain1, 0) + COALESCE(AgentcommissionRetain2, 0)
                                - COALESCE(AgentcommissionRetainRej1, 0) - COALESCE(AgentcommissionRetain2Rej, 0)
                                - COALESCE(AgentcommissionRetainCan1, 0) - COALESCE(AgentcommissionRetainCan2, 0)
                                - COALESCE(AgentcommissionRetainCancelLikeReject1, 0)
                                - COALESCE(AgentcommissionRetainCancelLikeReject2, 0),
        FxFee = COALESCE(FxFee1, 0) + COALESCE(FxFee2, 0) - COALESCE(FxFeeRej1, 0) - COALESCE(FxFee2Rej, 0),
        FxFeeM = COALESCE(FxFeeM1, 0) + COALESCE(FxFeeM2, 0) - COALESCE(FxFeeRejM1, 0) - COALESCE(FxFeeM2Rej, 0),
        FxFeeR = COALESCE(FxFeeR1, 0) + COALESCE(FxFeeR2, 0) - COALESCE(FxFeeRejR1, 0) - COALESCE(FxFeeR2Rej, 0),
        UnclaimedNumTrans = COALESCE(UnclaimedNumTrans1, 0) + COALESCE(UnclaimedNumTransClosed, 0),
        UnclaimedAmount = COALESCE(UnclaimedAmount1, 0) + COALESCE(UnclaimedAmountClosed, 0),
        UnclaimedCOGS = COALESCE(UnclaimedCOGS1, 0) + COALESCE(UnclaimedCOGSClosed, 0),
        BankCommission = COALESCE(BankCommission1, 0) + COALESCE(BankCommission2, 0) - COALESCE(BankCommission3, 0)
                         - COALESCE(BankCommission4, 0) - COALESCE(BankCommission5, 0) - COALESCE(BankCommission6, 0),
        PayerCommission = COALESCE(PayerCommission1, 0) + COALESCE(PayerCommission2, 0) - COALESCE(PayerCommission3, 0)
                          - COALESCE(PayerCommission4, 0) - COALESCE(PayerCommission5, 0)
                          - COALESCE(PayerCommission6, 0),
        IncomeFee = COALESCE(IncomeFee1, 0) + COALESCE(IncomeFee2, 0),
        NetFee = (COALESCE(IncomeFee1, 0) + COALESCE(IncomeFee2, 0))
                 - (COALESCE(IncomeFeeRej1, 0) + COALESCE(IncomeFee2Rej, 0) + COALESCE(IncomeFeeCan1, 0)
                    + COALESCE(IncomeFeeCan2, 0)
                   )
                 - ((COALESCE(CashDiscount1, 0) + COALESCE(CashDiscount2, 0))
                    - (COALESCE(CashDiscount3, 0) + COALESCE(CashDiscount4, 0))
                    - (COALESCE(CashDiscount5, 0) + COALESCE(CashDiscount6, 0))
                   ),
        FeeCanR = COALESCE(IncomeFeeRej1, 0) + COALESCE(IncomeFee2Rej, 0) + COALESCE(IncomeFeeCan1, 0)
                  + COALESCE(IncomeFeeCan2, 0),
        CashDiscount = (COALESCE(CashDiscount1, 0) + COALESCE(CashDiscount2, 0))
                       - (COALESCE(CashDiscount3, 0) + COALESCE(CashDiscount4, 0))
                       - (COALESCE(CashDiscount5, 0) + COALESCE(CashDiscount6, 0)),
        DCTran = COALESCE(DCTr1, 0) + COALESCE(DCTr2, 0),
        MerchantFee = COALESCE(MerchantFee1, 0) + COALESCE(MerchantFee2, 0);





    IF @Type = 1
    BEGIN
        UPDATE #Result
        SET AgentcommissionMonthly = AgentcommissionMonthly - FxFeeM
        WHERE AgentcommissionMonthly > 0;

        UPDATE #Result
        SET AgentcommissionRetain = AgentcommissionRetain - FxFeeR
        WHERE AgentcommissionRetain > 0;
    END;
    ELSE
    BEGIN
        UPDATE #Result
        SET AgentcommissionMonthly = AgentcommissionMonthly - FxFeeM,
            AgentcommissionRetain = AgentcommissionRetain - FxFeeR;
    END;


    UPDATE #Result
    SET NumNet = NumTrans - NumCancel,
        AmountNet = AmountTrans - AmountCancel,
        Result = FxResult + NetFee - AgentcommissionMonthly - AgentcommissionRetain - FxFee - PayerCommission
                 + UnclaimedAmount - UnclaimedCOGS - MerchantFee; ---BankCommission
    UPDATE #Result
    SET CogsNet = AmountNet - FxResult;
    UPDATE #Result
    SET OtherCharges = 0
    WHERE OtherCharges IS NULL;
    UPDATE #Result
    SET OtherChargesD = 0
    WHERE OtherChargesD IS NULL;
    UPDATE #Result
    SET OtherChargesC = 0
    WHERE OtherChargesC IS NULL;
    UPDATE #Result
    SET NetResult = Result + OtherCharges,
        CogsTrans = CogsCancel + CogsNet;



    ---------------------------------------------- Calculo de Other Products
    CREATE TABLE #tOtherProd
    (
        [idAgent] INT,
        [AgentName] VARCHAR(100),
        [AgentCode] VARCHAR(50),
        [Total] INT,
        [CancelsTotal] INT,
        [TotalNet] INT,
        [Amount] MONEY,
        [CGS] MONEY,
        [Fee] MONEY,
        [FeeM] MONEY,
        [FeeR] MONEY,
        [ProviderComm] MONEY,
        [CorpCommission] MONEY,
        [AgentCommMonthly] MONEY,
        [AgentCommRetain] MONEY,
        [FX] MONEY,
        [CheckFees] MONEY,      /*2015-Ago-15*/
        [ReturnedFee] MONEY,
        [TransactionFee] MONEY, /*2015-Sep-21*/
        [CustomerFee] MONEY,    /*2015-Sep-21*/
        [ProccessingFee] MONEY, /*2020-Jul*/
        [ScannerFee] MONEY      /*2015-Sep-21*/
    );
    



    ---------------------------------------------- Calculo DepositAgent
    SELECT IdAgent,
           SUM(DepositAgent) DepositAgent
    INTO #tDepositAgent
    FROM
    (
        SELECT ab.IdAgent,
               ISNULL((SUM(   CASE
                                  WHEN DebitOrCredit = 'Credit' THEN
                                      Amount
                                  ELSE
                                      0
                              END
                          ) - SUM(   CASE
                                         WHEN DebitOrCredit = 'Debit' THEN
                                             Amount
                                         ELSE
                                             0
                                     END
                                 )
                      ) * FactorNew,
                      0
                     ) DepositAgent
        FROM #TempAgents
            INNER JOIN AgentBalance ab
                ON ab.IdAgent = #TempAgents.IdAgent
            INNER JOIN Agent ag
                ON ab.IdAgent = ag.IdAgent
                   AND ag.IdAgentBankDeposit NOT IN ( 42, 43, 46 )
            LEFT JOIN #bankcommission bc
                ON bc.DateOfBankCommission = CAST(DATEADD(dd, - (DAY(DateOfMovement) - 1), DateOfMovement) AS DATE)
        WHERE ab.TypeOfMovement = 'DEP'
              AND DateOfMovement >= @StartDate
              AND DateOfMovement < @EndDate
        GROUP BY ab.IdAgent,
                 bc.FactorNew
    ) cteD
    GROUP BY IdAgent;
    ---------------------------------------------- Calculo DepositAgent



    ------------------------------Output --------------------------------------------------
    SELECT t.IdAgent,
           AgentCode,
           AgentName,
           NumTrans,
           NumCancel,                                                                         --No mostrar
           NumNet,
           AmountTrans,
           AmountCancel,
           AmountNet,
           CogsTrans,
           CogsCancel,
           CogsNet,                                                                           --No mostrar
           FxResult,
           IncomeFee,
           t.FeeCanR,
           t.CashDiscount,
           t.NetFee,
           AgentcommissionMonthly,
           AgentcommissionRetain,
           FxFeeM,
           FxFeeR,
           ISNULL(SpecialCommission, 0) SpecialCommission,                                    --No mostrar
                                                                                              ----
           PayerCommission,                                                                   --Cambio en el calculo
                                                                                              ----
           UnclaimedAmount,
           UnclaimedCOGS,
           OtherCharges,
           OtherChargesC,
           OtherChargesD,                                                                     --No mostrar
           Result,
                                                                                              ----
           NetResult - ISNULL(SpecialCommission, 0) NetResult,                                --Cambio en el calculo --Profit
           CASE
               WHEN NumNet != 0 THEN
           (NetResult - ISNULL(SpecialCommission, 0)) / NumNet
               ELSE
                   0
           END Margin,                                                                        --Cambio en el calculo
                                                                                              ----
           ISNULL(UserName, '') Parent,
           SalesRep,                                                                          --No mostrar
           CASE
               WHEN @Type = 2 THEN
                   CASE
                       WHEN ISNULL(c.CountryCode, '') = 'HTI' THEN
                           'HAI'
                       WHEN ISNULL(c.CountryCode, '') = 'PRY' THEN
                           'PAR'
                       ELSE
                           ISNULL(c.CountryCode, '')
                   END
               WHEN @Type = 3 THEN
                   CASE
                       WHEN ISNULL(t.IdCountryCurrency, 0) != 0 THEN
                           CASE
                               WHEN ISNULL(c2.CountryCode, '') = 'HTI' THEN
                                   'HAI' + '/' + ISNULL(cu.CurrencyCode, '')
                               WHEN ISNULL(c2.CountryCode, '') = 'PRY' THEN
                                   'PAR' + '/' + ISNULL(cu.CurrencyCode, '')
                               ELSE
                                   ISNULL(c2.CountryCode, '') + '/' + ISNULL(cu.CurrencyCode, '')
                           END
                       ELSE
                           ''
                   END
               ELSE
                   ''
           END CountryCode,
                                                                                              --------------------
           c.CountryName,
           (AgentcommissionMonthly + AgentcommissionRetain + FxFeeM + FxFeeR + ISNULL(SpecialCommission, 0))
           + OtherChargesC - OtherChargesD CommSeller,
           DepositAgent BkFeesSeller,
           [OtherProducts] OtherProductsSeller,
           (FxResult + NetFee)
           - ((AgentcommissionMonthly + AgentcommissionRetain + FxFeeM + FxFeeR + ISNULL(SpecialCommission, 0))
              + OtherChargesC - OtherChargesD
             ) - PayerCommission - DepositAgent + [OtherProducts] - MerchantFee ProfitSeller, ---W2+X2
           CASE
               WHEN ISNULL(NumNet, 0) > 0 THEN
           ((FxResult + NetFee)
            - ((AgentcommissionMonthly + AgentcommissionRetain + FxFeeM + FxFeeR + ISNULL(SpecialCommission, 0))
               + OtherChargesC - OtherChargesD
              ) - PayerCommission - DepositAgent + [OtherProducts] - MerchantFee
           )
           / NumNet
               ELSE
                   0
           END MarginSeller,
           t.DCTran AS DCTran,
           t.MerchantFee

    --------------------
    FROM
    (
        SELECT #Result.IdAgent,
               IdCountry,
               IdCountryCurrency,
               #Result.AgentCode,
               #Result.AgentName,
               NumTrans,
               NumCancel,
               NumNet,
               AmountTrans,
               AmountCancel,
               AmountNet,
               CogsTrans,
               CogsCancel,
               CogsNet,
               FxResult,
               IncomeFee,
               AgentcommissionMonthly,
               AgentcommissionRetain,
               FxFeeM,
               FxFeeR,
               CASE
                   WHEN PayerCommission > 0 THEN
                       PayerCommission
                   ELSE
                       0
               END AS PayerCommission,
               UnclaimedAmount,
               UnclaimedCOGS,
               OtherCharges,
               OtherChargesC,
               OtherChargesD,
               Result,
               FxResult + NetFee - AgentcommissionMonthly - AgentcommissionRetain - FxFeeM - FxFeeR
               - CASE
                     WHEN PayerCommission > 0 THEN
                         PayerCommission
                     ELSE
                         0
                 END - UnclaimedAmount + UnclaimedCOGS - OtherChargesC + OtherChargesD - MerchantFee AS NetResult,
               (
                   SELECT IdUserSellerParent FROM Seller WHERE IdUserSeller = IdSalesRep
               ) AS IdUserSellerParent,
               SalesRep,
               BankCommission,
               ISNULL(#tOtherProd.[CorpCommission], 0.0) [OtherProducts],
               ISNULL(#tDepositAgent.[DepositAgent], 0.0) [DepositAgent],
               FeeCanR,
               CashDiscount,
               DCTran,
               NetFee,
               MerchantFee
        FROM #Result
            -- Join with #tOtherProd to get CorpCommission for each agent
            LEFT JOIN #tOtherProd
                ON #Result.IdAgent = #tOtherProd.idAgent
            -- Join with #tDepositAgent to get DepositAgent for each agent
            LEFT JOIN
            (
                SELECT IdAgent,
                       SUM(DepositAgent) AS DepositAgent
                FROM
                (
                    SELECT ab.IdAgent,
                           ISNULL((SUM(   CASE
                                              WHEN DebitOrCredit = 'Credit' THEN
                                                  Amount
                                              ELSE
                                                  0
                                          END
                                      ) - SUM(   CASE
                                                     WHEN DebitOrCredit = 'Debit' THEN
                                                         Amount
                                                     ELSE
                                                         0
                                                 END
                                             )
                                  ) * bc.FactorNew,
                                  0
                                 ) AS DepositAgent
                    FROM #TempAgents
                        INNER JOIN AgentBalance ab
                            ON ab.IdAgent = #TempAgents.IdAgent
                        INNER JOIN Agent ag
                            ON ab.IdAgent = ag.IdAgent
                               AND ag.IdAgentBankDeposit NOT IN ( 42, 43, 46 )
                        LEFT JOIN #bankcommission bc
                            ON bc.DateOfBankCommission = CAST(DATEADD(dd, - (DAY(DateOfMovement) - 1), DateOfMovement) AS DATE)
                    WHERE ab.TypeOfMovement = 'DEP'
                          AND DateOfMovement >= @StartDate
                          AND DateOfMovement < @EndDate
                    GROUP BY ab.IdAgent,
                             bc.FactorNew
                ) cteD
                GROUP BY IdAgent
            ) AS #tDepositAgent
                ON #Result.IdAgent = #tDepositAgent.IdAgent

    ----------------------------------------------------
    ) t
        LEFT JOIN Users u
            ON u.IdUser = ISNULL(IdUserSellerParent, 0)
        LEFT JOIN #tempSC s
            ON s.IdAgent = t.IdAgent
        LEFT JOIN Country c
            ON t.IdCountry = c.IdCountry
        LEFT JOIN CountryCurrency cc
            ON t.IdCountryCurrency = cc.IdCountryCurrency
        LEFT JOIN Country c2
            ON c2.IdCountry = cc.IdCountry
        LEFT JOIN Currency cu
            ON cu.IdCurrency = cc.IdCurrency
    ORDER BY AgentCode,
             CASE
                 WHEN @Type = 2 THEN
                     CASE
                         WHEN ISNULL(c.CountryCode, '') = 'HTI' THEN
                             'HAI'
                         WHEN ISNULL(c.CountryCode, '') = 'PRY' THEN
                             'PAR'
                         ELSE
                             ISNULL(c.CountryCode, '')
                     END
                 WHEN @Type = 3 THEN
                     CASE
                         WHEN ISNULL(t.IdCountryCurrency, 0) != 0 THEN
                             CASE
                                 WHEN ISNULL(c2.CountryCode, '') = 'HTI' THEN
                                     'HAI' + '/' + ISNULL(cu.CurrencyCode, '')
                                 WHEN ISNULL(c2.CountryCode, '') = 'PRY' THEN
                                     'PAR' + '/' + ISNULL(cu.CurrencyCode, '')
                                 ELSE
                                     ISNULL(c2.CountryCode, '') + '/' + ISNULL(cu.CurrencyCode, '')
                             END
                         ELSE
                             ''
                     END
                 ELSE
                     ''
             END;
END TRY
BEGIN CATCH
    DECLARE @ErrorMessage NVARCHAR(4000);
    DECLARE @ErrorSeverity INT;
    DECLARE @ErrorState INT;
    SELECT @ErrorMessage = ERROR_MESSAGE(),
           @ErrorSeverity = ERROR_SEVERITY(),
           @ErrorState = ERROR_STATE();
    RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH;
