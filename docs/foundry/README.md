# Foundry agent over the Fabric data agent

## Architecture
User → Foundry agent `energy-orchestrator` (gpt-5-mini) → **Fabric IQ (OneLake Catalog)** tool, called over MCP → Fabric data agent `da_energy_sustainability` → Direct Lake semantic model `sm_energy_sustainability` → Gold warehouse.

- Requests run as the signed-in user (delegated auth), so semantic-model RLS still applies.
- The data agent, not the semantic model, is connected: its tested instructions, grounding rules and evaluation stay in the path.
- The older Foundry "Microsoft Fabric" tool retires on 6 Nov 2026; this setup uses its replacement.

## Setup
| Item | Value |
|---|---|
| Foundry resource | `fdy-energy-ai`, Italy North, basic agent setup (no own Cosmos DB / AI Search / Storage) |
| Project | `proj-energy-agent` (new Foundry portal) |
| Model | `gpt-5-mini`, Global Standard, pay per token (Data Zone EU was not available for this model/region) |
| Roles | Foundry User + Foundry Project Manager on the resource (subscription Owner is control-plane only) |
| Tool | Fabric IQ → `da_energy_sustainability` (must be published) |

## Orchestrator instructions
```
You are an energy & carbon assistant for European electricity (IT, FR, DE, NL).
For any question about electricity data (carbon intensity, renewable share, generation mix, prices, cross-border flows, KPI targets, weather), call the Fabric data agent "da_energy_sustainability" and base your answer only on its result.
Do not compute, estimate or add numbers yourself. If the data agent says the data is not available, say so; never fill the gap with general knowledge.
Return the data agent's answer as it is. You may shorten it, but do not reword, add or combine offers, periods, values, units or countries.
For questions outside electricity data, answer briefly and say they are not covered by the data.
```

## What tracing the MCP call showed
- The orchestrator passed questions through unchanged (`arguments: {"userQuestion": ...}`).
- The literal "Data Through Date" in answers came from the **data agent** → fixed in its instructions.
- A garbled follow-up offer was added by the **orchestrator** → fixed with the pass-through rule above.
- About 5,100–5,300 orchestrator tokens per question, plus the data agent's CU on the Fabric capacity.

## Data residency notes
- Global Standard processing can run in any Azure region; Data Zone (EU) or regional deployments keep it in scope where available.
- The OneLake Catalog tool warns that data may leave the Azure compliance boundary once it reaches Foundry.
- Agent 365 traces may be stored across regions.
- For production: Application Insights for distributed traces; standard agent setup (own Cosmos DB for threads) where compliance requires it.
