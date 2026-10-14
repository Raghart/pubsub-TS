import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";
import amqp from "amqplib";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";

export const handlerPause = (gs: GameState): (ps: PlayingState) => AckType => {
    return (ps: PlayingState) : AckType => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
};

export const handlerMove = (gs: GameState, ch: amqp.ConfirmChannel): (am: ArmyMove) => Promise<AckType> => {
    return async (am: ArmyMove) : Promise<AckType> => {
        const moveOutcome = handleMove(gs, am);
        console.log(`Moved ${am.units} to ${am.toLocation}`);
        process.stdout.write("> ");
        switch (moveOutcome) {
            case MoveOutcome.Safe: {
                return AckType.Ack;
            }
            case MoveOutcome.MakeWar: {
                try {
                    publishJSON(
                    ch, ExchangePerilTopic, `${WarRecognitionsPrefix}.${gs.getUsername()}`, {
                    attacker: am.player,
                    defender: gs.getPlayerSnap(),
                } satisfies RecognitionOfWar);
                } catch (err) {
                    console.log(`Error publishing the war recognition: ${err}`);
                    return AckType.NackRequeue;
                } finally {
                    return AckType.Ack;
                }
            };
            case MoveOutcome.SamePlayer: {
                return AckType.NackDiscard;
            }
            default: {
                return AckType.NackDiscard;
            };
        };
    };
};

export const handlerWar = (gs: GameState) : (rw: RecognitionOfWar) => AckType => {
    return (rw: RecognitionOfWar) => {
        const warResolution = handleWar(gs, rw);
        process.stdout.write("> ")
        switch (warResolution.result) {
            case WarOutcome.NotInvolved: {
                return AckType.NackRequeue;
            };
            case WarOutcome.NoUnits: {
                return AckType.NackDiscard;
            };
            case WarOutcome.OpponentWon: {
                return AckType.Ack;
            };
            case WarOutcome.YouWon: {
                return AckType.Ack;
            };
            case WarOutcome.Draw: {
                return AckType.Ack
            };
            default: {
                console.log(`war resolution unknown: ${warResolution}`);
                return AckType.NackDiscard;
            }
        }
    }
};