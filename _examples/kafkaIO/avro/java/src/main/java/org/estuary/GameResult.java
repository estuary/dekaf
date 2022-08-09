package org.estuary;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class GameResult {
    public abstract String getTeam();

    public abstract String getUser();

    public abstract Long getScore();

    public abstract Long getFinished_at();

    @SchemaCreate
    public static GameResult create(String team, String user, Long score, Long finished_at) {
        return new AutoValue_GameResult(team, user, score, finished_at);
    }
}
