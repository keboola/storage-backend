<?php

namespace Keboola\Datatype\Definition;

interface DefinitionInterface
{
    /**
     * @return string
     */
    public function getSQLDefinition();

    /**
     * @return array{type:string, length:string|null, nullable:bool, compression?:mixed}
     */
    public function toArray();

    /**
     * @return string
     */
    public function getBasetype();
}
