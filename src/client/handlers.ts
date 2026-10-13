import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import type { GameState, PlayingState } from "../internal/gamelogic/gamestate.js";
import { handleMove } from "../internal/gamelogic/move.js";
import { handlePause } from "../internal/gamelogic/pause.js";

export const handlerPause = (gs: GameState): (ps: PlayingState) => void => {
    return (ps: PlayingState) => {
        handlePause(gs, ps);
        process.stdout.write("> ");
    };
};

export const handlerMove = (gs: GameState): (am: ArmyMove) => void => {
    return (am: ArmyMove) => {
        handleMove(gs, am);
        console.log(`Moved ${am.units} to ${am.toLocation}`);
        process.stdout.write("> ");
    }
}