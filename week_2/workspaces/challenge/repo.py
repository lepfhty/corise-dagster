from dagster import repository
import workspaces.challenge.week_2_challenge as w2


@repository
def repo():
    return [w2.week_2_challenge_docker,
            w2.week_2_challenge_single_op_docker]
