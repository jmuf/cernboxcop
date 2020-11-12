package cmd

import (
	"testing"
)

func TestDemangle(t *testing.T) {
	type tuple struct {
		path     string
		token    string
		expected string
	}

	tuples := []*tuple{
		&tuple{
			"__myprojects/cryogenics-group/Photos/jamaica.png",
			"",
			"/eos/project/c/cryogenics-group/Photos/jamaica.png",
		},
		&tuple{
			"SubWG_Experiments/EXP_resources_subgroup_comments.xlsx",
			"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJX.eyJleHAiOjE2MDUwMDA5MjcsImFjY291bnRfaWQiOiJnY2F2YWxsbyIsImdyb3VwcyI6W10sImRpc3BsYXlfbmFtZSI6IkdyZWdvcnkgQ2F2YWxsbyAoZ2NhdmFsbG8pIn0.p5RBKjOZtQf9TRDsxQiiM4jWw22pcKIzlJMbypXZ_00",
			"/eos/user/g/gcavallo/SubWG_Experiments/EXP_resources_subgroup_comments.xlsx",
		},
		/*
			&tuple{
				"__myshares/AG2019 20(id:210627)/Club_members_AG.xlsx",
				"",
				"/eos/user/l/lburdzan/CSC/AG2019/Club_members_AG.xlsx",
			},
		*/
	}

	for _, tu := range tuples {
		got, err := demangle(tu.path, tu.token)
		if err != nil {
			t.Fatal(err)
		}
		if got != tu.expected {
			t.Fatalf("got:%s expected:%s", got, tu.expected)
		}
	}
}
