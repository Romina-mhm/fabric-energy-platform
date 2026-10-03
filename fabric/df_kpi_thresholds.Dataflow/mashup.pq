[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "kpi_threshold_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared kpi_threshold = let
  Source = Lakehouse.Contents(null),
  Navigation = Source{[workspaceId = "a9b50eeb-8564-4e09-ac18-87c3583d27a3"]}[Data],
  #"Navigation 1" = Navigation{[lakehouseId = "82f9a133-f37f-4546-9ebb-0ebf0a7c6fb8"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[Id = "Files", ItemKind = "Folder"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Name = "business"]}[Content],
  #"Navigation 4" = #"Navigation 3"{[Name = "kpi_thresholds.csv"]}[Content],
  #"Imported CSV" = Csv.Document(#"Navigation 4", [Delimiter = ",", Columns = 7, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(#"Imported CSV", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"country_code", type text}, {"carbon_intensity_alert_gco2e_per_kwh", type number}, {"price_spike_alert_eur_per_mwh", type number}, {"renewable_share_target_pct", type number}, {"valid_from", type date}, {"owner", type text}, {"note", type text}}),
  #"Uppercased text" = Table.TransformColumns(#"Changed column type", {{"country_code", each Text.Upper(_), type nullable text}})
in
  #"Uppercased text";
shared kpi_threshold_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = true, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "a9b50eeb-8564-4e09-ac18-87c3583d27a3"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "4a9c15b0-cd48-49fc-85c6-fdd5dcc0b185"]}[Data],
  Navigation_3 = Navigation_2{[Id = "dbo", ItemKind = "Schema"]}[Data],
  TableNavigation = Navigation_3{[Id = "silver_kpi_threshold", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
