import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consume.js";

export const handlerPause = (gs: GameState): (ps: PlayingState) => AckType => {
    return (ps: PlayingState) : AckType => {
        handlePause(gs, ps);
        process.stdout.write("> ");
        return AckType.Ack;
    };
};

export const handlerMove = (gs: GameState): (am: ArmyMove) => AckType => {
    return (am: ArmyMove) : AckType => {
        const moveOutcome = handleMove(gs, am);
        console.log(`Moved ${am.units} to ${am.toLocation}`);
        process.stdout.write("> ");

        switch (moveOutcome.toString()) {
            case "safe": {
                return AckType.Ack;
            }
            case "make war": {
                return AckType.Ack;
            };
            case "same player": {
                return AckType.NackDiscard;
            }
            default: {
                return AckType.NackDiscard;
            };
        };
    };
};