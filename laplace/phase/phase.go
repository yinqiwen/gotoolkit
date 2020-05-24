package phase

import (
	"context"
	"sync"
)

type PhaseFunc func(ctx context.Context, args []string) int

var phaseFuncs = make(map[string]PhaseFunc)

func RegisterPhaseFunc(name string, phase PhaseFunc) {
	phaseFuncs[name] = phase
}

type Phase struct {
	Name string   `toml:"name"`
	Args []string `toml:"args"`

	function PhaseFunc
}

func (p *Phase) init() error {
	f, exist := phaseFuncs[p.Name]
	if exist {
		p.function = f
	}
	return nil
}

func (p *Phase) Execute(ctx context.Context) error {
	p.function(ctx, p.Args)
	return nil
}

type PhaseUnit struct {
	Single   *Phase      `toml:"phase"`
	Parallel []PhaseUnit `toml:"parallel"`
}

func (p *PhaseUnit) init() error {
	if nil != p.Single {
		p.Single.init()
	} else {
		for _, phase := range p.Parallel {
			phase.init()
		}
	}
	return nil
}

func (p *PhaseUnit) Execute(ctx context.Context) error {
	if nil != p.Single {
		p.Single.Execute(ctx)
	} else {
		var waitGroup sync.WaitGroup
		waitGroup.Add(len(p.Parallel))
		defer waitGroup.Wait()

		for _, phase := range p.Parallel {
			go func(f *PhaseUnit) {
				defer waitGroup.Done()
				f.Execute(ctx)
			}(&phase)
		}
	}
	return nil
}

type ControlFlow struct {
	Name  string      `toml:"name"`
	Stage []PhaseUnit `toml:"stage"`
}

type Script struct {
	Desc        string
	ControlFlow ControlFlow `toml:"control_flow"`
}

func (s *Script) Init() error {
	for _, stage := range s.ControlFlow.Stage {
		stage.init()
	}
	return nil
}

func (s *Script) Execute(ctx context.Context) error {
	for _, stage := range s.ControlFlow.Stage {
		stage.Execute(ctx)
	}
	return nil
}
