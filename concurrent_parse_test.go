package avro_test

import (
	"sync"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/require"
)

func TestConcurrentParse(t *testing.T) {
	var wg sync.WaitGroup

	for i := 0; i < 10000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := avro.Parse(testSchema())
			require.NoError(t, err)
		}()
	}

	wg.Wait()
}

func testSchema() string {
	return `{
  "type": "record",
  "name": "FootballUpdateEvent",
  "namespace": "com.example.avro",
  "fields": [
    {
      "name": "event_metadata",
      "type": {
        "type": "record",
        "name": "EventMetadata",
        "fields": [
          { "name": "name", "type": "string", "default": "" },
          { "name": "trace", "type": "string", "default": "" },
          { "name": "stamp", "type": "long", "default": 0 },
          { "name": "destination", "type": "string", "default": "" }
        ]
      },
      "default": {
        "name": "",
        "trace": "",
        "stamp": 0,
        "destination": ""
      }
    },
    {
      "name": "player",
      "type": {
        "type": "record",
        "name": "Player",
        "fields": [
          { "name": "team_id", "type": "string", "default": "" },
          { "name": "name", "type": "string", "default": "" },
          { "name": "team", "type": "string", "default": "" },
          { "name": "contract", "type": "long", "default": 0 },
          { "name": "xg", "type": "double", "default": 0.0 },
          { "name": "xgp", "type": "double", "default": 0.0 },
          { "name": "xgp99", "type": "double", "default": 0.0 },
          { "name": "xg90", "type": "double", "default": 0.0 },
          {
            "name": "xg90p",
            "type": {
              "type": "record",
              "name": "MatchXG",
              "fields": [
                { "name": "matchXG", "type": "double", "default": 0.0 },
                { "name": "matchXGP", "type": "string", "default": "" }
              ]
            },
            "default": {
              "matchXG": 0.0,
              "matchXGP": ""
            }
          },
          {
            "name": "ttd",
            "type": "MatchXG",
            "default": {
              "matchXG": 0.0,
              "matchXGP": ""
            }
          },
          {
            "name": "leagueXG",
            "type": {
              "type": "record",
              "name": "LeagueXG",
              "fields": [
                { "name": "top_assist", "type": "string", "default": "" },
                { "name": "top_score", "type": "string", "default": "" },
                { "name": "top_xg", "type": "string", "default": "" },
                { "name": "top_creation", "type": "string", "default": "" }
              ]
            },
            "default": {
              "top_assist": "",
              "top_score": "",
              "top_xg": "",
              "top_creation": ""
            }
          },
          {
            "name": "player_numbers",
            "type": {
              "type": "array",
              "items": {
                "type": "record",
                "name": "PlayerNumber",
                "fields": [
                  { "name": "player_number", "type": "long", "default": 0 }
                ]
              }
            },
            "default": []
          },
          {
            "name": "contact_renewal",
            "type": [
              "null",
              {
                "type": "record",
                "name": "ContactRenewal",
                "fields": [
                  { "name": "stamp", "type": "long", "default": 0 },
                  { "name": "type", "type": "string", "default": "" },
                  { "name": "xg", "type": "MatchXG", "default": {
                    "matchXG": 0.0,
                    "matchXGP": ""
                  } }
                ]
              }
            ],
            "default": null
          },
          { "name": "defence", "type": "string", "default": "" },
          { "name": "offence", "type": "string", "default": "" }
        ]
      },
      "default": {}
    }
  ]
}`
}
